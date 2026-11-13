// Package agent — the SUPERVISION CHANNEL (F-30.4, slice 2).
//
// channelServer is the agent's side of the gRPC channel its per-job workers
// dial. One worker connection multiplexes both verbs: Signals
// (agent→worker opportunistic cancellation notice, server stream) and
// Telemetry (worker→agent metric samples, client stream), plus the
// ConnectJob handshake that ASSOCIATES the connection with a job. The
// channel carries CONTROL AND TELEMETRY ONLY — never the job payload (that
// crossed the wire exactly once, worker←API, via GetJob).
//
// Bind: the channel listens on a CONFIGURED address (channelDefaultBind —
// loopback — for the local-substrate posture, where the worker is a
// subprocess on the same host); a node-network agent (Pod workers) passes a
// routable bind so the channel is reachable across the cluster network.
//
// Association rides the PEER ADDRESS: the worker dials exactly once and all
// its RPCs share that connection, so the remote address is a stable
// per-worker identity. ConnectJob admits the worker only when the job is one
// of the agent's live children (the launch ledger is the custody proof);
// afterwards the worker's Telemetry samples are credited to that job.
//
// Folding: the agent accumulates each child's samples (step outcomes, log
// bytes, and the FINAL JobSample deposited by the worker's pre-exit flush).
// When the child exits, observeChild takes the account: a final JobSample
// supersedes the agent's exit-code inference (exact status + duration); the
// log-byte delta is folded into the agent's own cdrom_agent_log_bytes_total.
package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	supervisionpb "cdrom/internal/gen/cdrom/supervision/v1"
)

// channelDefaultBind is the supervision channel's DEFAULT bind: loopback
// (LOCAL-SUBSTRATE posture — the channel is intra-host by construction: the
// worker is a subprocess on the same host). A NODE-NETWORK agent (whose
// workers are Pods that cannot reach the agent's localhost) overrides this
// via the agent's ChannelBind config (e.g. ":7401" or "<pod-ip>:7401") so
// the channel is reachable across the cluster network.
const channelDefaultBind = "127.0.0.1:0"

// errChannelUnauthorized denies a channel verb from an unassociated peer.
var errChannelUnauthorized = errors.New("supervision channel: connection not associated (ConnectJob first)")

// stepSample is one forwarded step outcome (the account's step breakdown).
type stepSample struct {
	stepType string
	status   string
	dur      time.Duration
}

// childAccount is the agent's ACCUMULATION of one child's telemetry samples
// (F-30.4 slice 2): the sample count (accountability for the final flush's
// FlushAck), the STEP OUTCOMES the child reported (these cannot be inferred
// from the exit code — folding them into the agent's own series at the
// child's exit is the proof of absorption), the log bytes the child streamed
// (delta-tracked across the child's cumulative watermarks), and the child's
// FINAL account (status + duration) when the worker's pre-exit flush
// deposited one.
type childAccount struct {
	samples       int64
	steps         []stepSample
	lastWatermark int64
	maxSeq        uint32 // highest sample sequence seen (item B1 dedupe)
	logBytes      int64
	cancelAcks    uint32 // relayed cancellation notices the child acknowledged
	finalSeen     bool
	finalStatus   string
	finalDur      time.Duration
}

// channelServer serves the supervision channel for one agent.
type channelServer struct {
	supervisionpb.UnimplementedSupervisorServer
	// admit decides the ConnectJob handshake. ok=false ⇒ refuse (the worker
	// runs channel-less; PERMANENT — retrying cannot help). resumed=true ⇒
	// the worker was admitted WITHOUT being a live child (the agent
	// restarted mid-job; the ticket's consistency was validated instead).
	// jobToken is the ticket's token (the resume lane MAY verify its
	// signature via the API — slice 3 item B3).
	admit func(coord spawnCoordinate, ticketAgentName, jobToken string) (ok bool, resumed bool)
	log   *slog.Logger

	// Transport.
	lis  net.Listener
	srv  *grpc.Server
	bind string // requested listen address ("" = loopback default)
	addr string // advertised to workers (CDROM_PARENT_GRPC_ADDR); "" = disabled

	// Peer association (ConnectJob).
	assocMu sync.Mutex
	peers   map[string]spawnCoordinate // remote address → coordinate

	// Signal subscriptions (one per connected worker's Signals stream).
	subMu sync.Mutex
	subs  map[*signalSubscription]struct{}

	// Per-coordinate sample accumulation.
	acctMu   sync.Mutex
	accounts map[spawnCoordinate]*childAccount

	// Settler (slice 3, item B2): invoked when a TEARMED (adoptive)
	// coordinate's Telemetry stream ends WITH a final JobSample — the
	// adopted worker's completion signal (it has no local child to
	// Observe; the final flush is how the fresh instance learns it is
	// done). Nil = no settlements (pre-item-B construction).
	settler func(coord spawnCoordinate)
}

// signalSubscription is one worker's Signals stream subscription.
type signalSubscription struct {
	coord spawnCoordinate
	ch    chan *supervisionpb.Signal
}

// newChannelServer builds the channel server (not yet listening). bind is
// the address to LISTEN on (empty = the loopback default, channelDefaultBind
// — the local-substrate posture); a node-network agent passes a routable
// bind so its Pod workers can reach the channel. admit decides the ConnectJob
// handshake (ok, resumed — see admitChannelWorker).
func newChannelServer(bind string, admit func(spawnCoordinate, string, string) (bool, bool), log *slog.Logger) *channelServer {
	cs := &channelServer{
		admit:    admit,
		log:      log,
		bind:     bind,
		peers:    make(map[string]spawnCoordinate),
		subs:     make(map[*signalSubscription]struct{}),
		accounts: make(map[spawnCoordinate]*childAccount),
	}
	cs.srv = grpc.NewServer()
	supervisionpb.RegisterSupervisorServer(cs.srv, cs)
	return cs
}

// start binds the listener (the configured bind, or the loopback default)
// and serves. On failure the channel is DISABLED (addr ""); workers cope (no
// CDROM_PARENT_GRPC_ADDR ⇒ they run without the channel; the agent falls
// back to exit-code inference).
func (cs *channelServer) start() error {
	bind := cs.bind
	if bind == "" {
		bind = channelDefaultBind
	}
	lis, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	cs.lis = lis
	cs.addr = lis.Addr().String()
	go func() { _ = cs.srv.Serve(lis) }()
	return nil
}

// Addr returns the advertised loopback address ("" when disabled).
func (cs *channelServer) Addr() string {
	if cs == nil || cs.lis == nil {
		return ""
	}
	return cs.addr
}

// stop shuts the channel down (agent shutdown; in-flight children keep
// running DETACHED — their final flushes may be lost, which the agent's
// exit-code fallback absorbs; controlled drain ordering is slice 3).
func (cs *channelServer) stop() {
	if cs == nil || cs.srv == nil {
		return
	}
	cs.srv.Stop()
	_ = cs.lis.Close()
}

// --------------------------------------------------------------------------
// gRPC surface
// --------------------------------------------------------------------------

// ConnectJob is the worker's HANDSHAKE: it names its (job, attempt)
// coordinate and presents its job token + attribution name. Cold, the agent
// admits the worker only when the coordinate is one of its LIVE children
// (the launch ledger is the custody proof). On a RESUME (the agent
// restarted mid-job and no longer holds the live child), the agent admits
// the worker when the TICKET IS CONSISTENT with this agent: the coordinator
// names a real job and the token's agent_name claim equals the agent's OWN
// registered name (the token is IdP-signed and was spent against the API,
// so a forged one is worthless — it is the custody proof tying the worker
// to this agent lineage). Admission ASSOCIATES the worker's connection
// (peer address) with the coordinate for the Telemetry and Signals verbs.
func (cs *channelServer) ConnectJob(ctx context.Context, ticket *supervisionpb.JobTicket) (*supervisionpb.ConnectionGrant, error) {
	coord := spawnCoordinate{jobID: ticket.GetJobId(), attempt: ticket.GetAttempt()}
	ok, resumed := cs.admit(coord, ticket.GetAgentName(), ticket.GetJobToken())
	if !ok {
		return &supervisionpb.ConnectionGrant{Admitted: false}, nil
	}
	cs.assocMu.Lock()
	cs.peers[peerAddr(ctx)] = coord
	cs.assocMu.Unlock()
	if resumed {
		cs.log.Info("agent: worker RECONNECTED to the supervision channel (resume)", "job", coord.jobID, "attempt", coord.attempt)
	} else {
		cs.log.Info("agent: worker connected to the supervision channel", "job", coord.jobID, "attempt", coord.attempt)
	}
	return &supervisionpb.ConnectionGrant{Admitted: true, Resumed: resumed}, nil
}

// Signals is the agent-initiated SERVER stream: the worker subscribes once
// and the agent pushes opportunistic cancellation notices (Broadcast).
// Non-authoritative — the worker's own status poll remains the guaranteed
// path (F-05); the stream only shrinks reaction latency.
func (cs *channelServer) Signals(_ *supervisionpb.Empty, stream grpc.ServerStreamingServer[supervisionpb.Signal]) error {
	coord, ok := cs.peerCoord(stream.Context())
	if !ok {
		return errChannelUnauthorized
	}
	sub := &signalSubscription{coord: coord, ch: make(chan *supervisionpb.Signal, 4)}
	cs.subMu.Lock()
	cs.subs[sub] = struct{}{}
	cs.subMu.Unlock()
	defer func() {
		cs.subMu.Lock()
		delete(cs.subs, sub)
		cs.subMu.Unlock()
	}()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case sig := <-sub.ch:
			if err := stream.Send(sig); err != nil {
				return nil // the worker hung up; unsubscribe
			}
		}
	}
}

// Telemetry is the worker-initiated CLIENT stream: the worker pushes metric
// samples (step outcomes, log-byte watermarks) and, before exit, a FINAL
// flush (the JobSample + CloseSend). The agent credits the samples to the
// worker's job and acknowledges the flush with the sample count.
func (cs *channelServer) Telemetry(stream grpc.ClientStreamingServer[supervisionpb.Sample, supervisionpb.FlushAck]) error {
	coord, ok := cs.peerCoord(stream.Context())
	if !ok {
		return errChannelUnauthorized
	}
	var n int64
	for {
		sample, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		cs.credit(coord, sample)
		n++
	}
	ackErr := stream.SendAndClose(&supervisionpb.FlushAck{ReceivedSamples: n})
	// ADOPTED SETTLEMENT (item B2): the stream ended WITH a final JobSample
	// — for an ADOPTED coordinate (no live child to Observe) this is the
	// worker's completion signal: settle it (consume the account, roll the
	// metrics, discharge the adoption). Live children settle via
	// observeChild instead (the settler guards against both).
	cs.acctMu.Lock()
	fin := false
	if acc := cs.accounts[coord]; acc != nil {
		fin = acc.finalSeen
	}
	cs.acctMu.Unlock()
	if fin && cs.settler != nil {
		cs.settler(coord)
	}
	return ackErr
}

// peerCoord resolves the calling connection's coordinate via the ConnectJob
// association (peer address). Unknown peers (rogue dials, a worker that
// never shook hands) are unauthorized.
func (cs *channelServer) peerCoord(ctx context.Context) (spawnCoordinate, bool) {
	cs.assocMu.Lock()
	defer cs.assocMu.Unlock()
	coord, ok := cs.peers[peerAddr(ctx)]
	return coord, ok
}

// peerAddr extracts the remote address of a gRPC call (the per-worker
// connection identity).
func peerAddr(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	return p.Addr.String()
}

// --------------------------------------------------------------------------
// Credit & fold
// --------------------------------------------------------------------------

// credit books one sample into the child's account.
func (cs *channelServer) credit(coord spawnCoordinate, sample *supervisionpb.Sample) {
	cs.acctMu.Lock()
	defer cs.acctMu.Unlock()
	acc := cs.accounts[coord]
	if acc == nil {
		acc = &childAccount{}
		cs.accounts[coord] = acc
	}
	acc.samples++
	// SEQUENCE DEDUPE (slice 3, item B1): the worker stamps every sample
	// with a monotonic GLOBAL sequence; a mid-life reconnection replaces the
	// link, and a torn link's in-flight samples can land AFTER the new
	// link's (no ordering across connections). Crediting only sequences
	// ABOVE the high-water mark makes the fold IDEMPOTENT across epochs
	// (a sample is never double-booked; the watermark delta-tracking and
	// the single-final invariant compose with it). seq 0 = unsequenced
	// (credited as before).
	var seq uint32
	switch k := sample.GetKind().(type) {
	case *supervisionpb.Sample_Step:
		seq = k.Step.GetSeq()
	case *supervisionpb.Sample_Bytes:
		seq = k.Bytes.GetSeq()
	case *supervisionpb.Sample_Job:
		seq = k.Job.GetSeq()
	case *supervisionpb.Sample_Notice:
		seq = k.Notice.GetSeq()
	}
	if seq > 0 && seq <= acc.maxSeq {
		return
	}
	if seq > acc.maxSeq {
		acc.maxSeq = seq
	}
	switch s := sample.GetKind().(type) {
	case *supervisionpb.Sample_Step:
		acc.steps = append(acc.steps, stepSample{
			stepType: s.Step.GetStepType(),
			status:   s.Step.GetStatus(),
			dur:      s.Step.GetDuration().AsDuration(),
		})
	case *supervisionpb.Sample_Bytes:
		total := s.Bytes.GetTotalBytes()
		if total > acc.lastWatermark {
			acc.logBytes += total - acc.lastWatermark
			acc.lastWatermark = total
		}
	case *supervisionpb.Sample_Job:
		acc.finalSeen = true
		acc.finalStatus = s.Job.GetStatus()
		acc.finalDur = s.Job.GetDuration().AsDuration()
	case *supervisionpb.Sample_Notice:
		if s.Notice.GetKind() == supervisionpb.NoticeSample_CANCELLATION_ACK {
			acc.cancelAcks++
		}
	}
}

// setAdoptedSettler installs the adopted-coordinate settlement callback
// (item B2). See the settler field.
func (cs *channelServer) setAdoptedSettler(f func(spawnCoordinate)) {
	if cs == nil {
		return
	}
	cs.settler = f
}

// takeAccount consumes the child's accumulated account (cleans up the
// slot). Nil when the child deposited nothing (channel down, worker died
// before flushing) — the caller then falls back to exit-code inference.
func (cs *channelServer) takeAccount(coord spawnCoordinate) *childAccount {
	if cs == nil {
		return nil
	}
	cs.acctMu.Lock()
	defer cs.acctMu.Unlock()
	acc := cs.accounts[coord]
	delete(cs.accounts, coord)
	return acc
}

// broadcastCancellation pushes an opportunistic cancellation notice to every
// subscriber of the coordinate (non-blocking: a slow or already-dead worker
// simply misses it — its own status poll is the authority).
func (cs *channelServer) broadcastCancellation(coord spawnCoordinate) {
	if cs == nil {
		return
	}
	sig := &supervisionpb.Signal{Kind: &supervisionpb.Signal_Cancelled{
		Cancelled: &supervisionpb.CancelSignal{JobId: coord.jobID},
	}}
	cs.subMu.Lock()
	defer cs.subMu.Unlock()
	for sub := range cs.subs {
		if sub.coord != coord {
			continue
		}
		select {
		case sub.ch <- sig:
		default: // subscriber backlog: drop (the poll converges anyway)
		}
	}
}
