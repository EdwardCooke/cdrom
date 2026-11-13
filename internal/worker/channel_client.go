// Package worker — the worker's side of the LOCAL SUPERVISION CHANNEL
// (F-30.4, slice 2).
//
// The worker DIALS its parent agent's loopback channel (advertised in the
// environment as CDROM_PARENT_GRPC_ADDR) and keeps the single connection for
// its whole life: gRPC/HTTP2 multiplexes the ConnectJob handshake, the
// Signals subscription (opportunistic cancellation notice), and the
// Telemetry stream (metric samples + the final flush) on it. The channel
// carries CONTROL AND TELEMETRY ONLY — never the job payload.
//
// Degradation is total: when the channel is absent (no env var), unreachable,
// or refuses the handshake, the worker runs channel-less — its job executes
// exactly as before (the agent infers the outcome from the exit code). The
// channel NEVER gates the job.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	supervisionpb "cdrom/internal/gen/cdrom/supervision/v1"
	"cdrom/internal/metrics"
)

// telemetryWatermarkInterval is how often the worker pushes its cumulative
// log-byte watermark to the agent (between the final flushes). Coarse: the
// WATERMARK is cumulative, so a missed tick costs nothing.
const telemetryWatermarkInterval = 5 * time.Second

// flushTimeout bounds the final flush's wait for the agent's ack: the flush
// must never materially delay the worker's exit (the job already reported
// its terminal status to the API independently of the channel).
const flushTimeout = 2 * time.Second

// reconnectBaseDelay / reconnectMaxDelay pace the MID-LIFE reconnector
// (slice 3, item B1): a gentle ramp (15 s → 30 s → 60 s). A refused connect
// costs microseconds, so persisting against an EPHEMERAL-address agent (the
// local posture — the dead agent's address never comes back) is negligible,
// while a STABLE-address agent (the k8s posture) is reclaimed within one
// ramp step.
// Variables (not constants) so tests can shorten the ramp (house pattern,
// cf. the agent's pollInterval).
var (
	reconnectBaseDelay = 15 * time.Second
	reconnectMaxDelay  = 60 * time.Second
)

// SupervisorClient is the worker's supervision-channel FACADE to its parent
// agent. Constructed AFTER the payload claim (the handshake needs the
// execution's attempt). A NIL client (permanent refusal at startup) means
// the worker runs channel-less; a DEGRADED client (startup budget exhausted
// — the agent is not answering YET) runs channel-less UNTIL its reconnector
// establishes the link (item B1). The façade is STABLE across
// reincarnations: the recorder/callbacks the worker wires at startup keep
// working while the underlying LINK (connection + streams) tears and
// re-establishes.
type SupervisorClient struct {
	parentAddr string
	jobID      int64
	attempt    int32
	logger     *slog.Logger

	// agentName is the worker's ATTRIBUTION name (ticket material for the
	// RESUME admission: on a fresh agent instance the ticket's consistency
	// with the agent's own name is the custody proof).
	agentName string
	// token is the worker's JOB TOKEN (retained for re-handshakes).
	token string

	// Current incarnation (nil = down). Guarded by mu.
	mu   sync.Mutex
	link *link

	// Wake signaling for the reconnector (sync.Cond — no epoch/channel
	// races): kickLost sets needWake + broadcasts; the reconnector waits on
	// the cond until kicked or stopped.
	wakeMu   sync.Mutex
	wakeCond *sync.Cond
	needWake bool

	// stopped is closed when the façade winds down (the worker's exit): it
	// halts the reconnector and the watermark loop.
	stopped  chan struct{}
	stopOnce sync.Once

	// seq is the MONOTONIC sample sequence (GLOBAL across incarnations — the
	// agent dedups by (coordinate, seq), so a replaced link never
	// double-counts a sample).
	seq atomic.Uint32

	// Cumulative log bytes (watermarks are deltas on the agent side).
	logBytes atomic.Int64

	// Buffered finals (sent on the flush).
	finalMu   sync.Mutex
	finalJob  *supervisionpb.JobSample
	finalSeen bool

	// Cancellation notice (Signals): invoked once when the agent broadcasts
	// a Cancelled for this job (opportunistic; the worker's own status poll
	// remains the authority, F-05). Facade-level: survives reincarnations.
	cancelNotifyMu sync.Mutex
	cancelNotify   func()
	notified       bool
}

// link is ONE incarnation of the channel connection: the gRPC client conn,
// the Telemetry stream, and the outbound pump (gRPC streams are not safe
// for concurrent Send: the recorder ENQUEUEs; one goroutine SENDS; the
// sentinel drains the pump before the final CloseSend).
type link struct {
	sc    *SupervisorClient
	cc    *grpc.ClientConn
	cli   supervisionpb.SupervisorClient
	tel   grpc.ClientStreamingClient[supervisionpb.Sample, supervisionpb.FlushAck]
	out   chan *sampleEnvelope
	down  chan struct{} // closed when the pump retires (flush barrier)
	dead  chan struct{} // closed when the link is torn (kick/shutdown)
	deadOnce sync.Once
}

// sampleEnvelope is one outbound sample (or the drain sentinel).
type sampleEnvelope struct {
	sample   *supervisionpb.Sample
	sentinel bool
}

// connectBudget bounds the worker's STARTUP resilience to the agent's
// channel coming up slightly late (the agent boots its listener during its
// own startup; a worker claiming its payload can race that — and in the
// k8s posture the agent Pod can respawn before the worker's first connect).
// Transient failures (connect refused, dial timeout) are retried with
// backoff until the budget runs out; a PERMANENT refusal (the agent ANSWERED
// and declined the ticket — inconsistency with this agent) stops
// immediately: retrying cannot help. Five seconds comfortably spans a local
// agent's channel bring-up while bounding the degraded-path cost (the
// channel NEVER gates the job).
const connectBudget = 5 * time.Second

// NewSupervisorClient dials the parent agent's channel, shakes hands
// (ConnectJob with the job token + the worker's attribution name), and opens
// the Telemetry stream + Signals subscription. It returns (nil, nil) — NOT
// an error — when the channel is unavailable BY CONFIGURATION or PERMANENTLY
// refuses the handshake: the worker then runs channel-less FOREVER. When the
// channel is TRANSIENTLY unreachable (budget exhausted), it returns a
// DEGRADED client whose reconnector keeps reclaiming it in the background
// (item B1) — the worker runs channel-less UNTIL the link establishes. The
// only RETURNED error is a catastrophic one (caller may log it).
func NewSupervisorClient(parentAddr string, jobID int64, attempt int32, jobToken string, agentName string, logger *slog.Logger) (*SupervisorClient, error) {
	if parentAddr == "" || jobID == 0 {
		return nil, nil // channel-less by configuration
	}
	sc := &SupervisorClient{
		parentAddr: parentAddr,
		jobID:      jobID,
		attempt:    attempt,
		token:      jobToken,
		agentName:  agentName,
		logger:     logger,
		stopped:    make(chan struct{}),
	}
	sc.wakeCond = sync.NewCond(&sc.wakeMu)
	deadline := time.Now().Add(connectBudget)
	backoff := 250 * time.Millisecond
	first := true
	for {
		l, permDenied, ferr := sc.connectLink()
		if l != nil {
			sc.install(l)
			sc.startLoops()
			return sc, nil
		}
		if permDenied {
			logger.Info("worker: supervision channel not admitted; running channel-less", "job", jobID)
			return nil, nil
		}
		if time.Now().After(deadline) {
			logger.Warn("worker: supervision channel unreachable (budget exhausted); running channel-less (reconnector active)", "job", jobID, "parent", parentAddr, "err", ferr)
			sc.startLoops() // degraded: the reconnector persists gently
			// Seed the reconnector: the startup epoch's loss channel is
			// armed but UNCLOSED (no link tore), so park the reconnector
			// awake by kicking it once — it then re-dials on the gentle
			// ramp until the agent (re)appears at the advertised address.
			sc.kickLost("initial connect failed (degraded start)")
			return sc, nil
		}
		if !first {
			logger.Info("worker: supervision channel unavailable; retrying", "job", jobID, "err", ferr, "backoff", backoff)
		}
		first = false
		time.Sleep(backoff)
		if backoff *= 2; backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
}

// connectLink performs ONE connect attempt. Returns (link, nil, nil) on
// success; (nil, true, nil) on a PERMANENT refusal (the agent answered and
// declined the ticket); (nil, false, err) on a transient failure.
func (sc *SupervisorClient) connectLink() (*link, bool, error) {
	l := &link{
		sc:   sc,
		out:  make(chan *sampleEnvelope, 64),
		down: make(chan struct{}),
		dead: make(chan struct{}),
	}
	conn, err := grpc.NewClient(sc.parentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, false, fmt.Errorf("dial: %w", err)
	}
	l.cli = supervisionpb.NewSupervisorClient(conn)
	// Handshake on a TIMEOUT context; the long-lived streams (Telemetry,
	// Signals) ride context.Background() — their lifetime IS the connection
	// (closing the conn ends them). Binding them to the handshake's context
	// would cancel them the instant the constructor returns (latent bug,
	// caught by TestChannelEndToEndFold).
	hsCtx, hsCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer hsCancel()
	grant, herr := l.cli.ConnectJob(hsCtx, &supervisionpb.JobTicket{
		JobId:     sc.jobID,
		JobToken:  sc.token,
		Attempt:   sc.attempt,
		AgentName: sc.agentName,
	})
	if herr != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("handshake: %w", herr)
	}
	if !grant.GetAdmitted() {
		_ = conn.Close()
		return nil, true, nil // PERMANENT: the agent declined the ticket
	}
	if grant.GetResumed() {
		sc.logger.Info("worker: supervision channel RESUMED after the agent's restart", "job", sc.jobID)
	}
	l.cc = conn
	tel, terr := l.cli.Telemetry(context.Background())
	if terr != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("telemetry open: %w", terr)
	}
	l.tel = tel
	go l.pump()
	go l.subscribeSignals()
	return l, false, nil
}

// install publishes the new link and tears the displaced one down.
func (sc *SupervisorClient) install(l *link) {
	sc.mu.Lock()
	old := sc.link
	sc.link = l
	sc.mu.Unlock()
	if old != nil {
		old.tearDown()
	}
}

// startLoops begins the facade-level loops (watermark, reconnector) — once.
func (sc *SupervisorClient) startLoops() {
	go sc.watermarkLoop()
	go sc.reconnectLoop()
}

// kickLost tears the CURRENT link down and wakes the reconnector (COND
// broadcast — no epoch races). Reasonable to call redundantly (idempotent).
func (sc *SupervisorClient) kickLost(reason string) {
	sc.mu.Lock()
	l := sc.link
	sc.link = nil
	sc.mu.Unlock()
	if l != nil {
		l.tearDown()
	}
	sc.wakeMu.Lock()
	sc.needWake = true
	sc.wakeCond.Broadcast()
	sc.wakeMu.Unlock()
	sc.logger.Info("worker: supervision channel lost; reconnector engaged", "job", sc.jobID, "reason", reason)
}

// reconnectLoop is the MID-LIFE RECONNECTOR (item B1): when the link tears
// (agent death), it re-dials the advertised address on a gentle ramp and
// re-handshakes (the agent's RESUME lane admits the ticket). Persists until
// the worker stops — a stable-address agent is reclaimed quickly; an
// ephemeral-address one (local posture) is never reclaimed, at negligible
// cost (a refused connect is microseconds).
func (sc *SupervisorClient) reconnectLoop() {
	delay := reconnectBaseDelay
	for {
		// Wait for a kick (or the stop).
		sc.wakeMu.Lock()
		for !sc.needWake {
			sc.wakeCond.Wait()
		}
		stopped := false
		select {
		case <-sc.stopped:
			stopped = true
		default:
		}
		sc.needWake = false
		sc.wakeMu.Unlock()
		if stopped {
			return
		}
		for {
			select {
			case <-sc.stopped:
				return
			case <-time.After(delay):
			}
			l, permDenied, ferr := sc.connectLink()
			if l != nil {
				sc.install(l)
				sc.logger.Info("worker: supervision channel RECONNECTED (mid-life)", "job", sc.jobID)
				delay = reconnectBaseDelay
				break
			}
			if permDenied {
				// A RESPONDING agent declined the ticket. Mid-life this is
				// NOT fatal (a successor incarnation may admit it): keep the
				// gentle rhythm.
				sc.logger.Info("worker: supervision channel declined; will retry", "job", sc.jobID)
			} else {
				sc.logger.Debug("worker: supervision channel reconnect failed; will retry", "job", sc.jobID, "err", ferr)
			}
			if delay < reconnectMaxDelay {
				delay *= 2
			}
		}
	}
}

// tearDown closes the link's resources (idempotent). NOTE: it does NOT close
// cc — the pump's sentinel path must still be able to CloseAndRecv on the
// stream; the conn is closed by shutdown()/install() displacement.
func (l *link) tearDown() {
	l.deadOnce.Do(func() {
		close(l.down)
		close(l.dead)
	})
}

// Recorder returns the metrics.Recorder the worker plugs into the shared
// execution (target.Context.Metrics): step outcomes are forwarded live, log
// bytes accumulate toward the watermarks, and the job's terminal account is
// buffered for the final flush. Nil-safe: a nil client (channel-less
// posture) yields a nil recorder.
func (sc *SupervisorClient) Recorder() metrics.Recorder {
	if sc == nil {
		return nil
	}
	return &channelRecorder{sc: sc}
}

// SetCancelNotify arms the opportunistic cancellation callback (called once
// when the agent broadcasts a Cancelled for this job).
func (sc *SupervisorClient) SetCancelNotify(fn func()) {
	if sc == nil {
		return
	}
	sc.cancelNotifyMu.Lock()
	sc.cancelNotify = fn
	sc.cancelNotifyMu.Unlock()
}

// Flush performs the FINAL FLUSH against the CURRENT link: it enqueues the
// buffered job account + the last watermark, drains the pump, closes the
// send direction, and waits for the agent's ack (bounded by flushTimeout).
// Best-effort: a DOWN link (the agent died at exit) SKIPs the flush — the
// agent falls back to exit-code inference (total-degradation doctrine); a
// failure is logged, never propagated (the job's outcome already reached
// the API).
func (sc *SupervisorClient) Flush() {
	if sc == nil {
		return
	}
	sc.mu.Lock()
	l := sc.link
	sc.mu.Unlock()
	if l == nil {
		sc.logger.Info("worker: final flush skipped (channel down)", "job", sc.jobID)
		sc.shutdown()
		return
	}
	// Buffer the finals. The JobSample's SEQ is assigned HERE (at flush
	// time), AFTER the bytes sample's: the agent dedups by (coordinate,
	// seq), so the final must carry the HIGHEST sequence of the burst —
	// assigning it at RecordJob time would let the intervening bytes
	// sample steal a higher seq and swallow the final.
	l.enqueue(sc.bytesSample())
	sc.finalMu.Lock()
	if sc.finalSeen && sc.finalJob != nil {
		fj := sc.finalJob
		l.enqueue(&supervisionpb.Sample{Kind: &supervisionpb.Sample_Job{
			Job: &supervisionpb.JobSample{Status: fj.GetStatus(), Duration: fj.GetDuration(), Seq: sc.nextSeq()},
		}})
	}
	sc.finalMu.Unlock()
	// Drain barrier: the sentinel is SENT after everything enqueued before
	// it (chan FIFO), so when the pump retires it the finals are on the wire.
	l.enqueueSentinel()
	select {
	case <-l.down:
	case <-time.After(flushTimeout):
	}
	// Close the send direction and await the ack (bounded). CloseAndRecv is
	// the single terminating call on a client stream (it closes AND waits).
	type ackResult struct {
		ack *supervisionpb.FlushAck
		err error
	}
	resultCh := make(chan ackResult, 1)
	go func() {
		a, err := l.tel.CloseAndRecv()
		resultCh <- ackResult{a, err}
	}()
	select {
	case r := <-resultCh:
		if r.err != nil {
			sc.logger.Info("worker: final flush not acknowledged", "job", sc.jobID, "err", r.err)
		} else {
			sc.logger.Info("worker: final flush acknowledged", "job", sc.jobID, "samples", r.ack.GetReceivedSamples())
		}
	case <-time.After(flushTimeout):
		sc.logger.Info("worker: final flush ack timed out (continuing)", "job", sc.jobID)
	}
	sc.shutdown()
}

// shutdown winds the FAÇADE down (halts the reconnector + watermark loop;
// tears the current link; idempotent).
func (sc *SupervisorClient) shutdown() {
	sc.stopOnce.Do(func() {
		close(sc.stopped)
		sc.mu.Lock()
		if l := sc.link; l != nil {
			l.tearDown()
			_ = l.cc.Close()
			sc.link = nil
		}
		sc.mu.Unlock()
	})
}

// push delivers one sample to the CURRENT link (drops when down — telemetry
// is best-effort; the final flush's barrier still holds on the live link).
func (sc *SupervisorClient) push(sample *supervisionpb.Sample) {
	sc.mu.Lock()
	l := sc.link
	sc.mu.Unlock()
	if l == nil {
		return
	}
	l.enqueue(sample)
}

// nextSeq assigns the next monotonic sequence (attached to every sample —
// the agent dedups by (coordinate, seq) across incarnations).
func (sc *SupervisorClient) nextSeq() uint32 {
	return sc.seq.Add(1)
}

// enqueue is non-blocking: a saturated outbound buffer DROPs the sample
// (telemetry is best-effort; the final flush's barrier still holds because
// the sentinel is enqueued last and the buffer drains in order — a drop
// only sheds a non-final sample).
func (l *link) enqueue(sample *supervisionpb.Sample) {
	select {
	case l.out <- &sampleEnvelope{sample: sample}:
	case <-l.dead:
	default:
	}
}

// enqueueSentinel queues the drain sentinel (blocking until space or death —
// the flush's barrier depends on it being QUEUED).
func (l *link) enqueueSentinel() {
	select {
	case l.out <- &sampleEnvelope{sentinel: true}:
	case <-l.dead:
	}
}

// pump sends enqueued samples on the Telemetry stream (single sender). A
// SEND ERROR tears the link (engaging the reconnector — item B1).
func (l *link) pump() {
	defer l.tearDown()
	for env := range l.out {
		if env.sentinel {
			return
		}
		if err := l.tel.Send(env.sample); err != nil {
			// The channel died: the reconnector re-establishes it. Shed the
			// rest (the flush's barrier still retires — the sentinel is
			// always enqueued by Flush, and the pump consumes it here to
			// unwind).
			l.sc.logger.Info("worker: supervision channel telemetry down; shedding samples (reconnector engaged)", "job", l.sc.jobID, "err", err)
			l.sc.kickLost("telemetry send failed")
			for {
				select {
				case e := <-l.out:
					if e.sentinel {
						return
					}
				case <-l.dead:
					return
				}
			}
		}
	}
}

// subscribeSignals receives the agent's Signals stream (opportunistic
// cancellation notice) on THIS link. Tears the link (engaging the
// reconnector) when the stream/connection dies.
func (l *link) subscribeSignals() {
	stream, err := l.cli.Signals(context.Background(), &supervisionpb.Empty{})
	if err != nil {
		l.sc.kickLost("signals open failed")
		return
	}
	for {
		sig, err := stream.Recv()
		if err != nil {
			l.sc.kickLost("signals stream ended")
			return
		}
		if c := sig.GetCancelled(); c != nil && c.GetJobId() == l.sc.jobID {
			// ACK the relay THROUGH THE CHANNEL (F-30.4 slice 2): the
			// acknowledgement rides Telemetry into the agent's durable
			// exported metrics (cdrom_agent_cancel_signal_acks_total) —
			// proof of delivery that outranks the worker's log line (which
			// is subject to ordinary log durability). Enqueued BEFORE the
			// callback acts: the callback's consequences culminate in the
			// final flush's sentinel, and the FIFO positions the ack ahead
			// of it, so the ack always ships when the channel is alive.
			l.sc.push(&supervisionpb.Sample{
				Kind: &supervisionpb.Sample_Notice{
					Notice: &supervisionpb.NoticeSample{
						Kind: supervisionpb.NoticeSample_CANCELLATION_ACK,
						Seq:  l.sc.nextSeq(),
					},
				},
			})
			l.sc.fireCancelNotify()
		}
	}
}

// fireCancelNotify invokes the armed callback ONCE (idempotent).
func (sc *SupervisorClient) fireCancelNotify() {
	sc.cancelNotifyMu.Lock()
	fn, already := sc.cancelNotify, sc.notified
	if !already && fn != nil {
		sc.notified = true
	}
	sc.cancelNotifyMu.Unlock()
	if fn != nil && !already {
		fn()
	}
}

// watermarkLoop pushes the cumulative log-byte watermark on an interval
// (facade-level: aims at the CURRENT link).
func (sc *SupervisorClient) watermarkLoop() {
	ticker := time.NewTicker(telemetryWatermarkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.stopped:
			return
		case <-ticker.C:
			sc.push(sc.bytesSample())
		}
	}
}

func (sc *SupervisorClient) bytesSample() *supervisionpb.Sample {
	return &supervisionpb.Sample{Kind: &supervisionpb.Sample_Bytes{
		Bytes: &supervisionpb.BytesSample{TotalBytes: sc.logBytes.Load(), Seq: sc.nextSeq()},
	}}
}

// channelRecorder is the metrics.Recorder the shared execution draws on:
// step outcomes forward live, log bytes accumulate, the job's terminal
// account buffers for the final flush. Discarding when the channel is down.
type channelRecorder struct {
	sc *SupervisorClient
}

// RecordStep forwards one step's terminal outcome (type/status/duration).
func (r *channelRecorder) RecordStep(stepType, status string, d time.Duration) {
	r.sc.push(&supervisionpb.Sample{Kind: &supervisionpb.Sample_Step{
		Step: &supervisionpb.StepSample{
			StepType: stepType,
			Status:   status,
			Duration: durationpb.New(d),
			Seq:      r.sc.nextSeq(),
		},
	}})
}

// AddLogBytes accumulates the streamed log bytes (watermark source).
func (r *channelRecorder) AddLogBytes(n int64) {
	r.sc.logBytes.Add(n)
}

// RecordJob buffers the job's terminal account for the final flush (it is
// NOT pushed eagerly: the agent consumes it at the child's exit, and the
// flush guarantees its delivery before the worker terminates). Its sequence
// is assigned at FLUSH time (highest of the burst — see Flush).
func (r *channelRecorder) RecordJob(status string, d time.Duration) {
	r.sc.finalMu.Lock()
	r.sc.finalJob = &supervisionpb.JobSample{Status: status, Duration: durationpb.New(d)}
	r.sc.finalSeen = true
	r.sc.finalMu.Unlock()
}

// SetInflight is a no-op on the channel recorder: the AGENT owns the
// in-flight gauge (it counts its live children); the worker's own gauge
// would collide with the agent's exported series.
func (r *channelRecorder) SetInflight(n int) {}
