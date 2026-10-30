// Package executor runs a job's execution spec: an ordered list of steps.
// Each step is agnostic about *how* it runs: a step's Type selects a
// StepHandler that executes it. A step with no type is dispatched to the
// handler registered under DefaultType (the built-in "shell" handler, which
// runs a command — directly, or through a user-chosen shell — in a working
// directory with an environment). A target (or a plugin) can register
// additional step types via RegisterStepType (e.g. "ansible", "terraform",
// "argo"); a step whose type is not registered on the target fails the job
// with a clear error.
//
// The concrete handlers live in internal/stephandlers (and, later, in
// plugins); this package is the generic dispatch engine and knows only the
// DefaultType name, not the handlers themselves. The package is shared by the
// long-lived worker and the ephemeral agent, and must run on both Windows and
// Linux.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"google.golang.org/protobuf/proto"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ErrTimeout is returned by Execute (and wrapped by step handlers) when a job
// or a step exceeds its declared timeout. Callers (the worker and agent) use
// errors.Is(err, ErrTimeout) to report the job as timed_out rather than
// failed. A step handler that runs a command should wrap the context's
// DeadlineExceeded error with ErrTimeout so the distinction survives the
// executor's per-step error wrapping.
var ErrTimeout = errors.New("executor: timed out")

// ErrCancelled is returned by Execute (wrapped by a StepBarrier) when a job is
// cancelled while a worker is waiting at a step barrier. Callers (the worker)
// use errors.Is(err, ErrCancelled) to report the job as cancelled rather than
// failed. A job that is cancelled while a step is running is surfaced through
// the job's context cancellation instead (see the worker's user-cancel path).
var ErrCancelled = errors.New("executor: cancelled")

// StepOutputDirEnv is the environment variable the executor sets on every
// step: it names the per-step directory the step writes its output files into.
// After the step runs the executor reads every file in that directory and
// records each file's (trimmed) contents as a step output keyed by the file
// name — so a step produces an output simply by writing a file, and does not
// have to declare its outputs. The directory is created for every step, so a
// step handler that always produces output (e.g. the token_exchange handler)
// can write to it.
const StepOutputDirEnv = "CDROM_STEP_OUTPUT_DIR"

// Execute runs the steps of spec in order. A nil or empty spec succeeds
// without doing any work (a job with no execution spec is a no-op).
//
// For each step, in order:
//   - its condition (F-06) is evaluated against the condition context (the
//     step's own env, the prior steps' status/outputs, the upstream jobs'
//     status/outputs, and the job's identity); when it renders to false the
//     step is skipped (it does not run and does not fail the job), and when
//     it fails to parse or render that is a spec error that fails the job;
//   - the step runs (through the handler registered for its type);
//   - a step that fails or times out stops the job — its error is returned
//     (wrapping ErrTimeout on a timeout) — unless the step's IgnoreFailed is
//     set, in which case the failure is recorded and the job continues to the
//     next step (F-06).
//
// The provided ctx bounds the whole job. Two further deadlines may apply:
//   - a job-level timeout (spec.Timeout, when set) bounds the sum of all
//     steps; and
//   - a per-step timeout (step.Timeout, when set) bounds an individual step.
//
// When a deadline expires the running step is cancelled and Execute returns
// ErrTimeout (wrapped with context). A job with no timeouts runs unbounded
// (bounded only by ctx).
func Execute(ctx context.Context, spec *dbpb.JobSpec, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if spec == nil {
		return nil
	}
	// A job-level timeout derives a context that bounds the whole job (all
	// steps combined). A zero timeout means no job-level limit; the parent
	// ctx still bounds the job.
	jobCtx := ctx
	if timeout := spec.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		jobCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// The run's parameters (F-10) are interpolated into the job's spec before
	// it runs: each step's workdir, env values, and params are rendered as Go
	// templates against the run's parameter values (and the run's identity),
	// so the same spec can be reused across runs with different inputs. A
	// field that references a parameter with no value (no supplied value and
	// no default) is a spec error that fails the job, so an undefined
	// parameter never silently renders to an empty string. A job that is not
	// part of a run (or whose run carries no parameters) is left unchanged.
	run := RunInfoFromContext(ctx)
	spec, err := interpolateSpec(spec, run)
	if err != nil {
		return err
	}
	// The condition context is built up as the job runs: priorSteps accumulates
	// each step's status/outputs, while the upstream jobs and the job's
	// identity come from the context (set by the execution target, F-06), and
	// the run's parameters and identity (F-10) are available to a step's
	// condition.
	upstreamJobs := UpstreamJobsFromContext(ctx)
	identity := JobIdentityFromContext(ctx)
	// priorSteps accumulates each prior step's status/outputs, keyed by the
	// step's name, so a step's condition can reference a prior step by name
	// (F-06). A step with no name is keyed by its position ("step-<i>").
	priorSteps := make(map[string]StepConditionInfo, len(spec.GetSteps()))
	for i, step := range spec.GetSteps() {
		stepName := stepNameOf(step, i)
		// A step's condition (F-06) is evaluated before the step runs: when it
		// renders to false the step is skipped (it does not run and does not
		// fail the job); when it fails to parse or render, that is a spec
		// error and fails the job.
		condCtx := conditionContext(step, priorSteps, upstreamJobs, identity, run)
		skip, condErr := evaluateCondition(step, condCtx)
		if condErr != nil {
			err := fmt.Errorf("executor: step %q: condition: %w", stepName, condErr)
			reportStepStatus(ctx, stepName, StepStatusFailed, err.Error(), nil)
			return err
		}
		if skip {
			logger.Info("executor: step skipped (condition not met)", "step", stepName)
			reportStepStatus(ctx, stepName, StepStatusSkipped, "", nil)
			priorSteps[stepName] = StepConditionInfo{Name: stepName, Status: string(StepStatusSkipped)}
			continue
		}
		// A step with `once` set (F-01) runs on at most one target of a job
		// that fans out to a worker group: before running it, this target
		// claims a durable slot (job id + step name + attempt) through the
		// OnceClaimer. When another target already claimed it in the same
		// attempt, this target skips the step (recorded skipped, not failed).
		// A target with no claimer (an ephemeral agent, or a single-target
		// job) runs the step. The claim is made after the condition check, so
		// a condition-skipped step does not consume the slot.
		if step.GetOnce() {
			claimer := OnceClaimerFromContext(ctx)
			if claimer != nil {
				claimed, err := claimer.Claim(jobCtx, stepName)
				if err != nil {
					err := fmt.Errorf("executor: step %q: claim once: %w", stepName, err)
					reportStepStatus(ctx, stepName, StepStatusFailed, err.Error(), nil)
					return err
				}
				if !claimed {
					logger.Info("executor: step skipped (once: already claimed by another target)", "step", stepName)
					reportStepStatus(ctx, stepName, StepStatusSkipped, "", nil)
					priorSteps[stepName] = StepConditionInfo{Name: stepName, Status: string(StepStatusSkipped)}
					continue
				}
			}
		}
		// Every step gets a per-step output directory, exposed to it via
		// StepOutputDirEnv (F-06): the step writes one file per output it
		// produces into it, and the executor reads them all back after the
		// step runs (each file's name is the output's name). The directory is
		// created even for a step that produces no output, so a step handler
		// that always produces output (e.g. token_exchange) can write to it.
		stepToRun := step
		outDir, err := os.MkdirTemp("", "cdrom-step-outputs-")
		if err != nil {
			err := fmt.Errorf("executor: step %q: create output dir: %w", stepName, err)
			reportStepStatus(ctx, stepName, StepStatusFailed, err.Error(), nil)
			return err
		}
		stageExtra := map[string]string{StepOutputDirEnv: outDir}
		// The job's staged working area (F-21.6), when provisioned by the
		// execution target (via ContextWithStageArea), is exported to EVERY
		// step as StageAreaEnv: the default home for downloaded/extracted/
		// published packages — and addressable from any step (a shell step
		// that builds a tree before a publish_artifact step relies on it).
		// The target creates the directory once, at job start.
		if sa := StageAreaFromContext(ctx); sa != nil && sa.Func != nil {
			stageDir, serr := sa.Func()
			if serr != nil {
				err := fmt.Errorf("executor: step %q: create stage area: %w", stepName, serr)
				reportStepStatus(ctx, stepName, StepStatusFailed, err.Error(), nil)
				return err
			}
			stageExtra[StageAreaEnv] = stageDir
		}
		stepToRun = withEnv(step, stageExtra)
		// Carry the step's condition context (F-06) into the handler's context
		// so a step handler (the approval handler, F-13) can render a template
		// (e.g. an approval message) against the same data a step's condition
		// uses (secrets, prior steps' outputs, run parameters, upstream jobs).
		stepCtx := ContextWithConditionContext(jobCtx, condCtx)
		observeStep(ctx, step, stepName)
		stepStart := time.Now()
		stepErr := runStep(stepCtx, stepName, stepToRun, logger)
		stepStatus := StepStatusSucceeded
		if stepErr != nil {
			stepStatus = StepStatusFailed
			if errors.Is(stepErr, ErrTimeout) {
				stepStatus = StepStatusTimedOut
			}
		}
		finishStep(ctx, step, stepStatus, time.Since(stepStart))
		// Read the step's outputs from its output directory: every file the
		// step wrote into it becomes an output keyed by its file name.
		var outputs map[string]string
		if outDir != "" {
			outputs = readStepOutputs(outDir)
			_ = os.RemoveAll(outDir)
		}
		errMsg := ""
		if stepErr != nil {
			errMsg = stepErr.Error()
		}
		reportStepStatus(ctx, stepName, stepStatus, errMsg, outputs)
		priorSteps[stepName] = StepConditionInfo{Name: stepName, Status: string(stepStatus), Outputs: outputs}
		if stepErr != nil {
			// A failed/timed-out step stops the job unless it is ignore_failed
			// (F-06): the failure is recorded and the job continues.
			if step.GetIgnoreFailed() {
				logger.Info("executor: step failed but ignore_failed is set; continuing", "step", stepName)
				continue
			}
			return stepErr
		}
		// The step succeeded. If the job uses the cross-worker step barrier and
		// there is a next step, synchronize with the job's other workers: wait
		// until every worker alive at this step's start has completed it before
		// starting the next step. A worker that dies mid-step is dropped from
		// the barrier, so a dead worker cannot wedge the job. The barrier is
		// bounded by the job's context (the job-level timeout, when set), so a
		// job stuck at a barrier times out like any other hung job.
		if barrier := StepBarrierFromContext(ctx); barrier != nil && i+1 < len(spec.GetSteps()) {
			if err := barrier.SyncStep(jobCtx, stepName); err != nil {
				return err
			}
		}
	}
	return nil
}

// StepHandler executes a single step of a job. A handler is selected by the
// step's Type (an empty Type selects the handler registered under
// DefaultType). A handler receives the step's context — already bounded by
// the step's per-step timeout, when set — and the step itself, and returns a
// descriptive error when the step fails.
//
// Handlers are expected to be safe for concurrent use: a worker may run steps
// from different jobs on different goroutines.
type StepHandler func(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error

// DefaultType is the step type a step is dispatched to when its Type is
// empty. The built-in shell handler (internal/stephandlers) registers itself
// under this name, so a step with no type runs through the shell handler.
const DefaultType = "shell"

// TypeApproval is the step type of the built-in approval handler (F-13): a
// step of this type pauses the job at an approval gate until an authorized
// user approves or rejects it. The built-in handler (internal/stephandlers)
// registers itself under this name.
const TypeApproval = "approval"

// TypeEmail is the step type of the built-in email notification handler
// (F-18): a step of this type sends an SMTP message from the API. The
// built-in handler (internal/stephandlers) registers itself under this name.
const TypeEmail = "email"

// TypeWebhook is the step type of the built-in webhook notification handler
// (F-18): a step of this type performs an HTTP request from the API to the
// step's URL. The built-in handler (internal/stephandlers) registers itself
// under this name.
const TypeWebhook = "webhook"

// TypeDownloadArtifact is the step type of the built-in artifact-download
// handler (F-21.6): a step of this type copies a named artifact from the
// run's store namespace onto the target. The built-in handler
// (internal/stephandlers) registers itself under this name.
const TypeDownloadArtifact = "download_artifact"

// TypeExtract is the step type of the built-in archive-extraction handler
// (F-21.6): a step of this type unpacks a tar / gzipped tar / zip archive
// into a destination directory. The built-in handler (internal/stephandlers)
// registers itself under this name.
const TypeExtract = "extract"

// TypePublishArtifact is the step type of the built-in artifact-publishing
// handler (F-21.6): a step of this type collects the files matched by
// multiline glob patterns into a zip and uploads it to the job's own
// artifact namespace. The built-in handler (internal/stephandlers)
// registers itself under this name.
const TypePublishArtifact = "publish_artifact"

// TypeProbe is the step type of the built-in health-probe handler (F-22): a
// step of this type polls an HTTP(S) URL from the execution target until it
// responds with an accepted status (verification) or a deadline expires
// (failure). The built-in handler (internal/stephandlers) registers itself
// under this name.
const TypeProbe = "probe"

// TypeRollback is the step type of the built-in rollback handler (F-22): a
// step of this type asks the API to spawn a resumption of the job's own
// pipeline (restoring the suffix of the latest successful run from a named
// cut point) and optionally waits on its verdict. The built-in handler
// (internal/stephandlers) registers itself under this name.
const TypeRollback = "rollback"

// LogSink receives a job's step output (stdout/stderr) as it is produced, so
// an execution target can stream it to the API in near-real-time (F-02). A
// target that wants to stream output sets a sink in the context (via
// ContextWithLogSink) before calling Execute; step handlers that capture
// output (the built-in shell handler) write to it. When no sink is set, step
// output is not streamed (it still goes to the target's local stdout/stderr).
//
// Implementations must be safe for concurrent use and must not block: output
// is produced by the step's command, and a blocking sink would stall the
// command. A sink that cannot keep up should drop output — the persisted log
// (written by the API) is the source of truth for replay, and the UI
// resynchronizes from it on reconnect.
type LogSink interface {
	// WriteStepOutput records a chunk of a step's output. stepName is the
	// step's name (its identity in the job's spec); stream is "stdout" or
	// "stderr" (see the JobLogStreamStdout/Stderr constants in internal/api).
	WriteStepOutput(stepName string, stream string, data []byte)
}

// Output stream names a LogSink reports for a chunk of step output.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

type logSinkKey struct{}

type stepNameKey struct{}

// ContextWithLogSink returns a context that carries sink, so step handlers
// can stream a step's output to it. A nil sink returns ctx unchanged.
func ContextWithLogSink(ctx context.Context, sink LogSink) context.Context {
	if sink == nil {
		return ctx
	}
	return context.WithValue(ctx, logSinkKey{}, sink)
}

// LogSinkFromContext returns the LogSink carried by ctx, or nil when none is
// set. Step handlers call this to stream a step's output to the sink.
func LogSinkFromContext(ctx context.Context) LogSink {
	sink, _ := ctx.Value(logSinkKey{}).(LogSink)
	return sink
}

// StepNameFromContext returns the name of the step being executed, as set by
// runStep, or empty when absent. Step handlers use it to attribute streamed
// output to the right step.
func StepNameFromContext(ctx context.Context) string {
	name, _ := ctx.Value(stepNameKey{}).(string)
	return name
}

// StepBarrier synchronizes the workers of a job that fans out to a worker
// group at each step boundary (the cross-worker step barrier). After a worker
// completes a step, Execute calls SyncStep for that step: the worker reports
// its completion and then waits until every worker alive at the step's start
// has completed it, before starting the next step. A worker that dies
// mid-step is dropped from the barrier (it is no longer alive), so a dead
// worker cannot wedge the job.
//
// The barrier is set in the context (via ContextWithStepBarrier) only for a
// job that has the step-barrier flag set and targets a worker group; a job on
// a single target (an ephemeral agent) runs its steps unbarriered.
//
// Implementations must be safe for concurrent use. SyncStep is called with the
// job's context (already bounded by the job-level timeout, when set) and must
// return promptly when that context is cancelled (the worker is shutting down
// or the job was cancelled). When the job is cancelled while a worker is
// waiting, SyncStep returns ErrCancelled (wrapped with the context) so the
// worker can report the job as cancelled.
type StepBarrier interface {
	// SyncStep records that the worker completed stepName and blocks until the
	// barrier for that step is satisfied (every worker alive at the step's
	// start has completed it) or the job is cancelled (it returns ErrCancelled)
	// or ctx is done (it returns ctx.Err()).
	SyncStep(ctx context.Context, stepName string) error
}

type stepBarrierKey struct{}

// ContextWithStepBarrier returns a context that carries barrier, so Execute
// synchronizes the job's workers at each step boundary. A nil barrier returns
// ctx unchanged.
func ContextWithStepBarrier(ctx context.Context, barrier StepBarrier) context.Context {
	if barrier == nil {
		return ctx
	}
	return context.WithValue(ctx, stepBarrierKey{}, barrier)
}

// StepBarrierFromContext returns the StepBarrier carried by ctx, or nil when
// none is set.
func StepBarrierFromContext(ctx context.Context) StepBarrier {
	barrier, _ := ctx.Value(stepBarrierKey{}).(StepBarrier)
	return barrier
}

// TokenExchange lets a step handler request a new job token for a different
// audience (e.g. an outside resource the job needs to call) while the job is
// running. The execution target (worker or agent) implements it by calling the
// API's ExchangeJobToken RPC, presenting the job's own token and returning the
// exchanged token. A step handler that needs a token for an outside resource
// (the built-in "token_exchange" handler) obtains one from the context (via
// TokenExchangeFromContext) and hands it to the step's command (e.g. as an
// environment variable) so the command can authenticate to that resource.
//
// Implementations must be safe for concurrent use. Exchange is called with the
// step's context (already bounded by the step's per-step timeout, when set) and
// must return promptly when that context is done.
type TokenExchange interface {
	// Exchange returns a new job token scoped to the same job but the given
	// audience. expiresIn is how long the exchanged token should be valid for;
	// a zero value means the API's default exchanged-token lifetime.
	Exchange(ctx context.Context, audience string, expiresIn time.Duration) (string, error)
}

type tokenExchangeKey struct{}

// ContextWithTokenExchange returns a context that carries exchanger, so step
// handlers can request a job token for a different audience. A nil exchanger
// returns ctx unchanged.
func ContextWithTokenExchange(ctx context.Context, exchanger TokenExchange) context.Context {
	if exchanger == nil {
		return ctx
	}
	return context.WithValue(ctx, tokenExchangeKey{}, exchanger)
}

// TokenExchangeFromContext returns the TokenExchange carried by ctx, or nil
// when none is set. Step handlers call this to request an exchanged token.
func TokenExchangeFromContext(ctx context.Context) TokenExchange {
	exchanger, _ := ctx.Value(tokenExchangeKey{}).(TokenExchange)
	return exchanger
}

// Approval lets a step handler pause a job at an approval gate (F-13): it
// reports the job as awaiting approval (with a message shown to the user) and
// then waits for an authorized user to approve or reject it. The execution
// target (worker or agent) implements it by calling the API's
// ReportJobStatus (to report awaiting_approval + message) and CheckApproval
// (to poll for the decision), presenting the job's own token. A step handler
// that gates on approval (the built-in "approval" handler) obtains the gate
// from the context (via ApprovalFromContext) and blocks until the gate is
// resolved or the step's context is done.
//
// Implementations must be safe for concurrent use. Both methods are called
// with the step's context (already bounded by the step's per-step timeout, when
// set) and must return promptly when that context is done.
type Approval interface {
	// ReportAwaitingApproval reports that the job is paused at an approval
	// gate, showing message to the user (F-13). It is called once, before the
	// handler waits for a decision.
	ReportAwaitingApproval(ctx context.Context, message string) error
	// CheckApproval reports the current state of the job's approval gate:
	// whether it has been resolved (approved or rejected) or whether the job
	// has been cancelled. A handler waiting at the gate polls it until the
	// gate is resolved.
	CheckApproval(ctx context.Context) (CheckApprovalResult, error)
}

// CheckApprovalResult is the outcome of an Approval.CheckApproval call.
type CheckApprovalResult struct {
	// Resolved is true when the gate has been decided (approved or rejected).
	Resolved bool
	// Decision is the gate's outcome: "approved" or "rejected" (set only when
	// Resolved is true).
	Decision string
	// Cancelled is true when the job has been cancelled (a handler waiting at
	// the gate should stop and report the job cancelled).
	Cancelled bool
}

type approvalKey struct{}

// ContextWithApproval returns a context that carries gate, so step handlers
// can pause a job at an approval gate. A nil gate returns ctx unchanged.
func ContextWithApproval(ctx context.Context, gate Approval) context.Context {
	if gate == nil {
		return ctx
	}
	return context.WithValue(ctx, approvalKey{}, gate)
}

// ApprovalFromContext returns the Approval carried by ctx, or nil when none is
// set. Step handlers call this to pause a job at an approval gate.
func ApprovalFromContext(ctx context.Context) Approval {
	gate, _ := ctx.Value(approvalKey{}).(Approval)
	return gate
}

// Notifier lets a step handler send a notification through the API (F-18):
// the execution target renders the step's fields against the step's condition
// context and hands the rendered values to the API, which performs the actual
// send (SMTP for email, HTTP for webhook) using its own configuration. The
// target never opens an SMTP or outbound HTTP connection itself. The execution
// target (worker or agent) implements it by calling the API's SendEmail /
// SendWebhook RPCs, presenting the job's own token. A step handler that sends
// a notification (the built-in "email" and "webhook" handlers) obtains the
// notifier from the context (via NotifierFromContext).
//
// Implementations must be safe for concurrent use. Both methods are called
// with the step's context (already bounded by the step's per-step timeout, when
// set) and must return promptly when that context is done.
type Notifier interface {
	// SendEmail asks the API to send an SMTP message (F-18). The fields are the
	// step's rendered values (the target rendered them against the step's
	// condition context).
	SendEmail(ctx context.Context, email *EmailRequest) error
	// SendWebhook asks the API to perform an HTTP request to the step's URL
	// (F-18). The fields are the step's rendered values.
	SendWebhook(ctx context.Context, webhook *WebhookRequest) error
}

// EmailRequest is the rendered payload a notification step handler hands to the
// API's SendEmail RPC (F-18). The target renders the step's fields against the
// step's condition context and fills these in; the API supplies the sender and
// the SMTP server.
type EmailRequest struct {
	// To are the recipient addresses (required).
	To []string
	// CC are the carbon-copy recipients (optional).
	CC []string
	// BCC are the blind-carbon-copy recipients (optional).
	BCC []string
	// Subject is the message subject (optional).
	Subject string
	// Body is the message body (rendered by the target).
	Body string
	// ContentType selects how the body is sent: "plain", "html", or "markdown"
	// (rendered to HTML by the API). Empty means "markdown".
	ContentType string
	// From, when set, overrides the API's configured sender for this message.
	From string
	// ReplyTo, when set, sets the message's Reply-To header.
	ReplyTo string
}

// WebhookRequest is the rendered payload a notification step handler hands to
// the API's SendWebhook RPC (F-18). The target renders the step's fields
// against the step's condition context and fills these in; the API performs
// the HTTP request within its configured response timeout.
type WebhookRequest struct {
	// URL is the request URL (required).
	URL string
	// Method is the HTTP method (default "POST").
	Method string
	// Headers are the request headers, each a "Name: Value" entry (the target
	// renders each entry and splits it on the first ":" into name and value).
	Headers []string
	// Body is the request body (rendered by the target).
	Body string
	// ContentType selects how the body is sent: "plain" (sent as-is), "html"
	// (sent as-is), or "markdown" (rendered to HTML by the API). Empty means
	// "plain".
	ContentType string
	// Timeout, when set, is the response timeout the step requests; the API
	// clamps it to its configured maximum. When zero the API uses its
	// configured default.
	Timeout time.Duration
}

type notifierKey struct{}

// ContextWithNotifier returns a context that carries notifier, so step
// handlers can send a notification through the API. A nil notifier returns ctx
// unchanged.
func ContextWithNotifier(ctx context.Context, notifier Notifier) context.Context {
	if notifier == nil {
		return ctx
	}
	return context.WithValue(ctx, notifierKey{}, notifier)
}

// NotifierFromContext returns the Notifier carried by ctx, or nil when none is
// set. Step handlers call this to send a notification through the API.
func NotifierFromContext(ctx context.Context) Notifier {
	notifier, _ := ctx.Value(notifierKey{}).(Notifier)
	return notifier
}

// Restorer lets a step handler initiate a RESUMPTION of the job's own
// pipeline (F-22): the rollback step handler renders its params and asks the
// API to spawn a resumption run (anchored at the latest successful,
// non-rollback-origin run of the job's pipeline), then optionally waits on
// its verdict. The API applies the server-side guards (containment to the
// caller's own pipeline, no re-arm for rollback-origin callers, cooldown).
// The execution target (worker or agent) implements it by calling the API's
// InitiateRestore / GetRun RPCs, presenting the job's own token.
//
// Implementations must be safe for concurrent use. Methods are called with
// the step's context (already bounded by the step's per-step timeout, when
// set) and must return promptly when that context is done.
type Restorer interface {
	// InitiateRestore asks the API to spawn a resumption of the job's own
	// pipeline, cutting at fromJob (a stable job key, F-08). It returns the
	// new resumption run's id.
	InitiateRestore(ctx context.Context, fromJob string) (runID int64, err error)
	// RunStatus reports a run's current status (the polling aid for a
	// rollback step that waits on its resumption's verdict).
	RunStatus(ctx context.Context, runID int64) (statusStr string, err error)
}

type restorerKey struct{}

// ContextWithRestorer attaches a Restorer to the context for a job's step
// handlers. A nil restorer attaches nothing (the rollback step reports that
// restoration is unsupported by this execution target).
func ContextWithRestorer(ctx context.Context, restorer Restorer) context.Context {
	if restorer == nil {
		return ctx
	}
	return context.WithValue(ctx, restorerKey{}, restorer)
}

// RestorerFromContext retrieves the Restorer attached by
// ContextWithRestorer, or nil when none is set.
func RestorerFromContext(ctx context.Context) Restorer {
	r, _ := ctx.Value(restorerKey{}).(Restorer)
	return r
}

// ArtifactTransfer lets a step handler move files to and from the artifacts
// store through the API (F-21.6): the execution target (worker or agent)
// implements it by calling the API's artifact-proxy RPCs (DownloadArtifact /
// UploadArtifact), presenting the job's own token. The built-in
// "download_artifact" and "publish_artifact" handlers obtain the transfer
// from the context (via ArtifactTransferFromContext).
//
// Namespace addressing: a namespace is either a RAW store namespace (passed
// through as-is — the job's own id, for uploads) or a logical "run:<id>"
// pseudo-namespace (the API substitutes the run's store namespace for
// downloads). Targets never learn the run's namespace themselves; the API
// owns the mapping (it is the only component that can).
//
// Implementations must be safe for concurrent use. Both methods are called
// with the step's context (already bounded by the step's per-step timeout,
// when set) and must return promptly when that context is done.
type ArtifactTransfer interface {
	// DownloadArtifact copies the file named name in namespace onto the
	// target at dest (parents created; an existing file is replaced). It
	// returns the number of bytes transferred.
	DownloadArtifact(ctx context.Context, namespace, name, dest string) (int64, error)
	// UploadArtifact streams the file at src into the store as name in
	// namespace. It returns the number of bytes transferred.
	UploadArtifact(ctx context.Context, namespace, name, src string) (int64, error)
	// GetArtifact reports whether the file named name exists in namespace
	// (a metadata round trip). The download_artifact handler uses it to probe
	// its resolution candidates in preference order (F-21.6).
	GetArtifact(ctx context.Context, namespace, name string) error
}

type artifactTransferKey struct{}

// ContextWithArtifactTransfer returns a context that carries transfer, so
// step handlers can move files to/from the artifacts store. A nil transfer
// returns ctx unchanged.
func ContextWithArtifactTransfer(ctx context.Context, transfer ArtifactTransfer) context.Context {
	if transfer == nil {
		return ctx
	}
	return context.WithValue(ctx, artifactTransferKey{}, transfer)
}

// ArtifactTransferFromContext returns the ArtifactTransfer carried by ctx, or
// nil when none is set. Step handlers call this to download or upload
// artifacts.
func ArtifactTransferFromContext(ctx context.Context) ArtifactTransfer {
	transfer, _ := ctx.Value(artifactTransferKey{}).(ArtifactTransfer)
	return transfer
}

// StageAreaEnv is the environment variable the executor sets on every step
// when the execution target provisions a stage area (F-21.6): it names the
// job's staged working directory — the default home for downloaded and
// extracted packages and the default scan root for published ones. Ordinary
// jobs (no stage-dependent step) never see it.
const StageAreaEnv = "CDROM_STAGE_DIR"

// StageArea is the execution target's lazy provisioner for the job's staged
// working area (F-21.6). Func creates the directory on first use (typically
// os.MkdirTemp under the OS temp dir) and returns its absolute path; the
// executor invokes it once per job (memoized by the target) and exports the
// result to every step via StageAreaEnv. Clean, when non-nil, is invoked by
// the target once the job reaches a terminal state (best-effort removal).
type StageArea struct {
	// Func creates (or recalls) the stage directory. It must be safe to call
	// from multiple steps and must memoize internally.
	Func func() (string, error)
	// Clean removes the stage directory (best-effort; errors are logged, not
	// returned). Called once, after the job terminates.
	Clean func()
}

type stageAreaKey struct{}

// ContextWithStageArea returns a context that carries area, so the executor
// can export the job's staged working area to its steps. A nil area returns
// ctx unchanged.
func ContextWithStageArea(ctx context.Context, area *StageArea) context.Context {
	if area == nil {
		return ctx
	}
	return context.WithValue(ctx, stageAreaKey{}, area)
}

// StageAreaFromContext returns the StageArea carried by ctx, or nil when none
// is set.
func StageAreaFromContext(ctx context.Context) *StageArea {
	area, _ := ctx.Value(stageAreaKey{}).(*StageArea)
	return area
}

// OnceClaimer lets the executor enforce a step's `once` field (F-01): a step
// with `once` set runs on at most one target of a job that fans out to a
// worker group. The execution target (worker or agent) implements it by
// calling the API's ClaimStepOnce RPC (which claims a durable slot in the
// Database service, keyed by job id + step name + attempt), presenting the
// job's own token. The executor calls Claim before running a step with `once`
// set: when it returns false the step was already claimed by another target in
// the same attempt, so the executor skips the step (recorded skipped, not
// failed). A target that is not part of a fan-out (an ephemeral agent, or a
// group with one worker) returns true so the step runs.
//
// Implementations must be safe for concurrent use. Claim is called with the
// step's context (already bounded by the step's per-step timeout, when set)
// and must return promptly when that context is done.
type OnceClaimer interface {
	// Claim claims the "run this step" slot for stepName of the job. It
	// returns true when this target won the claim (it should run the step) and
	// false when another target already claimed it in the same attempt (this
	// target should skip the step).
	Claim(ctx context.Context, stepName string) (bool, error)
}

type onceClaimerKey struct{}

// ContextWithOnceClaimer returns a context that carries claimer, so the
// executor can enforce a step's `once` field. A nil claimer returns ctx
// unchanged.
func ContextWithOnceClaimer(ctx context.Context, claimer OnceClaimer) context.Context {
	if claimer == nil {
		return ctx
	}
	return context.WithValue(ctx, onceClaimerKey{}, claimer)
}

// OnceClaimerFromContext returns the OnceClaimer carried by ctx, or nil when
// none is set. The executor calls this to enforce a step's `once` field.
func OnceClaimerFromContext(ctx context.Context) OnceClaimer {
	claimer, _ := ctx.Value(onceClaimerKey{}).(OnceClaimer)
	return claimer
}

// ConditionContext is the data a step's condition (and, for the approval
// handler, its message, F-13) is rendered against: the step's own env, plus
// the structured keys "steps" (prior steps' status/outputs), "jobs" (upstream
// jobs' status/outputs), "job" (the job's identity), "params" (run parameters,
// F-10), "secrets" (named secrets as plaintext, F-12), and "run" (run
// identity, F-10). Execute builds it for each step and carries it in the
// context handed to the step's handler, so a handler (the approval handler)
// can render a template against the same context a step's condition uses.
type ConditionContext = map[string]any

type conditionContextKey struct{}

// ContextWithConditionContext returns a context that carries data, so a step
// handler can render a template (e.g. an approval message) against the step's
// condition context. A nil data returns ctx unchanged.
func ContextWithConditionContext(ctx context.Context, data ConditionContext) context.Context {
	if data == nil {
		return ctx
	}
	return context.WithValue(ctx, conditionContextKey{}, data)
}

// ConditionContextFromContext returns the condition context carried by ctx, or
// nil when none is set. Step handlers call this to render a template (e.g. an
// approval message) against the step's condition context.
func ConditionContextFromContext(ctx context.Context) ConditionContext {
	data, _ := ctx.Value(conditionContextKey{}).(ConditionContext)
	return data
}

// ---------------------------------------------------------------------------
// Condition context (F-06)
// ---------------------------------------------------------------------------

// StepConditionInfo is the condition-relevant view of a prior step of the
// same job: its name, terminal status, and the outputs it produced. A step's
// condition template can reference the prior steps via the "steps" key (a map
// keyed by step name).
type StepConditionInfo struct {
	// Name is the step's name (its identity in the job's spec).
	Name string
	// Status is the step's terminal status (succeeded, failed, skipped, or
	// timed_out).
	Status string
	// Outputs are the named values the step produced (each file it wrote into
	// its per-step output directory, keyed by file name); empty when the step
	// wrote no output files or did not run.
	Outputs map[string]string
}

// stepNameOf returns the name a step is referenced by: its declared name
// (step.Name) when set, or a positional fallback ("step<i>") when it has no
// name. The fallback keeps an unnamed step addressable in the condition
// context and in step results (it is a valid template field name, so
// `{{ .steps.step0.Outputs.x }}` works).
func stepNameOf(step *dbpb.JobStep, i int) string {
	if name := step.GetName(); name != "" {
		return name
	}
	return fmt.Sprintf("step%d", i)
}

// UpstreamJobInfo is the condition-relevant view of an upstream job higher in
// the pipeline chain: its id, name, status, and outputs. A step's condition
// template can reference the upstream jobs via the "jobs" key.
type UpstreamJobInfo struct {
	ID      int64
	Name    string
	Status  string
	Outputs map[string]string
}

// JobIdentity is the condition-relevant view of the job whose steps are
// running: its id, name, and status. A step's condition template can
// reference it via the "job" key.
type JobIdentity struct {
	ID     int64
	Name   string
	Status string
}

type upstreamJobsKey struct{}

type jobIdentityKey struct{}

// ContextWithUpstreamJobs returns a context that carries the upstream jobs
// (the status and outputs of the jobs this job depends on, F-06), so a step's
// condition can reference them. An empty list returns ctx unchanged.
func ContextWithUpstreamJobs(ctx context.Context, jobs []*dbpb.UpstreamJob) context.Context {
	if len(jobs) == 0 {
		return ctx
	}
	return context.WithValue(ctx, upstreamJobsKey{}, jobs)
}

// UpstreamJobsFromContext returns the upstream jobs carried by ctx, or nil
// when none are set.
func UpstreamJobsFromContext(ctx context.Context) []*dbpb.UpstreamJob {
	jobs, _ := ctx.Value(upstreamJobsKey{}).([]*dbpb.UpstreamJob)
	return jobs
}

// ContextWithJobIdentity returns a context that carries the job's identity, so
// a step's condition can reference the job's id/name/status.
func ContextWithJobIdentity(ctx context.Context, identity JobIdentity) context.Context {
	return context.WithValue(ctx, jobIdentityKey{}, identity)
}

// JobIdentityFromContext returns the job identity carried by ctx, or the zero
// value when none is set.
func JobIdentityFromContext(ctx context.Context) JobIdentity {
	identity, _ := ctx.Value(jobIdentityKey{}).(JobIdentity)
	return identity
}

// RunInfo is the run-scoped context a job's spec is interpolated against
// (F-10): the run's concrete parameter values (Params), the pipeline's named
// secrets as plaintext (Secrets, F-12), and the run's own identity (ID,
// PipelineID, Trigger, TriggerName). The execution target sets it in the
// context (via ContextWithRunInfo) before calling Execute, so the executor can
// render a job's spec fields (env, command, workdir) and a step's condition
// against the run's parameters and secrets. A job that is not part of a run
// (or whose run has no parameters or secrets) carries an empty RunInfo, and
// interpolation is a no-op for it.
type RunInfo struct {
	// Params are the run's concrete parameter values (a parameter not supplied
	// falls back to its default); empty when the run has no parameters.
	Params map[string]string
	// Secrets are the pipeline's named secrets as plaintext (F-12): a name ->
	// value map the API decrypted from the job's stored ciphertexts. A step's
	// spec references a secret as `{{ .secrets.name }}`. Empty when the
	// pipeline has no secrets.
	Secrets map[string]string
	// ID is the run's id; 0 when the job is not part of a run.
	ID int64
	// PipelineID is the pipeline the run executed; 0 when the job is not part
	// of a run.
	PipelineID int64
	// Trigger is how the run was started (e.g. "manual", "cron", "webhook", or
	// "event"); empty when the job is not part of a run.
	Trigger string
	// TriggerName is the name of the trigger that started the run; empty for a
	// manual run or a job not part of a run.
	TriggerName string
	// Artifacts is the run's resolved-artifact snapshot (F-21.6): a
	// spec.artifacts reference name → the STORED file name the reference
	// resolved to when the run started (the per-run snapshot, like the rest
	// of the spec — F-01/F-11). Denormalized from the run onto the job by the
	// API and handed to the target with the job. Empty for a job not part of
	// a run (or a run whose pipeline declares no spec.artifacts).
	Artifacts map[string]string
}

type runInfoKey struct{}

// ContextWithRunInfo returns a context that carries run, so Execute can
// interpolate the job's spec (and a step's condition) against the run's
// parameters and secrets. A RunInfo that carries no parameters, no secrets,
// and no run identity returns ctx unchanged.
func ContextWithRunInfo(ctx context.Context, run RunInfo) context.Context {
	if len(run.Params) == 0 && len(run.Secrets) == 0 && run.ID == 0 && run.PipelineID == 0 && run.Trigger == "" && run.TriggerName == "" && len(run.Artifacts) == 0 {
		return ctx
	}
	return context.WithValue(ctx, runInfoKey{}, run)
}

// RunInfoFromContext returns the RunInfo carried by ctx, or the zero value
// when none is set.
func RunInfoFromContext(ctx context.Context) RunInfo {
	run, _ := ctx.Value(runInfoKey{}).(RunInfo)
	return run
}

// conditionContext builds the data a step's condition template is rendered
// against (F-06): the step's own env (so `{{ .NAME }}` still works as before),
// plus the structured keys "steps" (the prior steps' status/outputs), "jobs"
// (the upstream jobs' status/outputs), "job" (the current job's identity),
// "params" (the run's parameter values, F-10), "secrets" (the pipeline's named
// secrets as plaintext, F-12), and "run" (the run's identity, F-10). The
// structured keys are set after the env so they win over any same-named env
// var.
func conditionContext(step *dbpb.JobStep, priorSteps map[string]StepConditionInfo, upstreamJobs []*dbpb.UpstreamJob, identity JobIdentity, run RunInfo) map[string]any {
	data := make(map[string]any, len(step.GetEnv())+7)
	for k, v := range step.GetEnv() {
		data[k] = v
	}
	data["steps"] = priorSteps
	data["jobs"] = toUpstreamJobInfos(upstreamJobs)
	// deps indexes the SAME upstream jobs by their STABLE KEYS (F-22): the
	// entries the API populated from the job's dependency observations carry
	// keys, so a branch job's condition routes on {{ .deps.<key>.status }} /
	// {{ .deps.<key>.outputs.<name> }}. Legacy entries (a live depends_on
	// scan) carry no key and fall back to the display name.
	if deps := toDependencyInfos(upstreamJobs); len(deps) > 0 {
		data["deps"] = deps
	}
	data["job"] = identity
	data["params"] = run.Params
	data["secrets"] = run.Secrets
	data["run"] = run
	return data
}

// toDependencyInfos indexes the upstream jobs by their stable keys (F-22),
// falling back to the display name for entries that carry no key.
func toDependencyInfos(jobs []*dbpb.UpstreamJob) map[string]UpstreamJobInfo {
	out := make(map[string]UpstreamJobInfo, len(jobs))
	for _, job := range jobs {
		info := UpstreamJobInfo{
			ID:      job.GetId(),
			Name:    job.GetName(),
			Status:  jobStatusName(job.GetStatus()),
			Outputs: job.GetOutputs(),
		}
		k := job.GetKey()
		if k == "" {
			k = job.GetName()
		}
		out[k] = info
	}
	return out
}

// toUpstreamJobInfos converts the proto upstream jobs into their
// condition-relevant view, keyed by the job's name (string status, so a
// condition can compare it directly). An empty list yields an empty map.
func toUpstreamJobInfos(jobs []*dbpb.UpstreamJob) map[string]UpstreamJobInfo {
	out := make(map[string]UpstreamJobInfo, len(jobs))
	for _, job := range jobs {
		out[job.GetName()] = UpstreamJobInfo{
			ID:      job.GetId(),
			Name:    job.GetName(),
			Status:  jobStatusName(job.GetStatus()),
			Outputs: job.GetOutputs(),
		}
	}
	return out
}

// jobStatusName maps a job status enum to the short name a condition template
// compares against (e.g. "succeeded", "failed").
func jobStatusName(status dbpb.JobStatus) string {
	switch status {
	case dbpb.JobStatus_JOB_STATUS_PENDING:
		return "pending"
	case dbpb.JobStatus_JOB_STATUS_RUNNING:
		return "running"
	case dbpb.JobStatus_JOB_STATUS_SUCCEEDED:
		return "succeeded"
	case dbpb.JobStatus_JOB_STATUS_FAILED:
		return "failed"
	case dbpb.JobStatus_JOB_STATUS_CANCELLED:
		return "cancelled"
	case dbpb.JobStatus_JOB_STATUS_TIMED_OUT:
		return "timed_out"
	case dbpb.JobStatus_JOB_STATUS_SKIPPED:
		return "skipped"
	default:
		return "unknown"
	}
}

// evaluateCondition renders step's Condition (a Go template, F-06) against
// data and parses the result as a boolean. It returns skip=true when the
// condition rendered to false (the step should be skipped); an empty Condition
// never skips. A condition that fails to parse as a template, fails to render,
// or whose rendered output does not parse as a boolean is a spec error (the
// job fails, the step is not skipped).
func evaluateCondition(step *dbpb.JobStep, data map[string]any) (skip bool, err error) {
	condition := step.GetCondition()
	if condition == "" {
		return false, nil
	}
	tmpl, err := template.New("condition").Parse(condition)
	if err != nil {
		return false, fmt.Errorf("parse template %q: %w", condition, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return false, fmt.Errorf("render template %q: %w", condition, err)
	}
	rendered := strings.TrimSpace(buf.String())
	value, err := strconv.ParseBool(rendered)
	if err != nil {
		return false, fmt.Errorf("condition %q rendered %q, which is not a boolean: %w", condition, rendered, err)
	}
	return !value, nil
}

// withEnv returns a copy of step with extra merged into its env (extra wins on
// a key collision). It is used to hand a step its per-step output directory
// (F-06) without mutating the caller's spec. A nil/empty extra returns step
// unchanged.
func withEnv(step *dbpb.JobStep, extra map[string]string) *dbpb.JobStep {
	if len(extra) == 0 {
		return step
	}
	clone := proto.Clone(step).(*dbpb.JobStep)
	env := make(map[string]string, len(step.GetEnv())+len(extra))
	for k, v := range step.GetEnv() {
		env[k] = v
	}
	for k, v := range extra {
		env[k] = v
	}
	clone.Env = env
	return clone
}

// ---------------------------------------------------------------------------
// Parameter interpolation (F-10)
// ---------------------------------------------------------------------------

// interpolateSpec returns a copy of spec with each step's workdir, env values,
// and params rendered as Go templates against the run's parameters (F-10), so
// the same spec can be reused across runs with different inputs. A field that
// references a parameter with no value (no supplied value and no default) is a
// spec error that fails the job, so an undefined parameter never silently
// renders to an empty string. A nil or empty spec, or a run with no
// parameters, is returned unchanged (a field with no template action renders
// to itself).
func interpolateSpec(spec *dbpb.JobSpec, run RunInfo) (*dbpb.JobSpec, error) {
	if spec == nil || len(spec.GetSteps()) == 0 {
		return spec, nil
	}
	data := interpolateData(run)
	steps := make([]*dbpb.JobStep, len(spec.GetSteps()))
	for i, step := range spec.GetSteps() {
		rendered, err := interpolateStep(step, data)
		if err != nil {
			return nil, fmt.Errorf("executor: step %q: %w", stepNameOf(step, i), err)
		}
		steps[i] = rendered
	}
	clone := proto.Clone(spec).(*dbpb.JobSpec)
	clone.Steps = steps
	return clone, nil
}

// interpolateStep returns a copy of step with its workdir, env values, and
// params rendered as Go templates against data (F-10). A field that fails to
// render (e.g. it references a parameter with no value) is a spec error.
//
// The approval handler's "message" param (F-13) is an exception: it is a
// template that may reference prior steps' outputs and upstream jobs (the full
// condition context), which this run-level interpolation does not have. It is
// left unrendered here and is rendered lazily by the approval handler against
// the step's condition context when the gate is reached.
func interpolateStep(step *dbpb.JobStep, data map[string]any) (*dbpb.JobStep, error) {
	clone := proto.Clone(step).(*dbpb.JobStep)
	if step.GetWorkdir() != "" {
		rendered, err := renderTemplate(step.GetWorkdir(), data)
		if err != nil {
			return nil, fmt.Errorf("workdir: %w", err)
		}
		clone.Workdir = rendered
	}
	if len(step.GetEnv()) > 0 {
		env := make(map[string]string, len(step.GetEnv()))
		for k, v := range step.GetEnv() {
			if v == "" {
				env[k] = v
				continue
			}
			rendered, err := renderTemplate(v, data)
			if err != nil {
				return nil, fmt.Errorf("env %q: %w", k, err)
			}
			env[k] = rendered
		}
		clone.Env = env
	}
	if len(step.GetParams()) > 0 {
		params := make(map[string]*dbpb.ParamValue, len(step.GetParams()))
		stepType := step.GetType()
		for k, v := range step.GetParams() {
			// The approval handler's message (F-13), the notification
			// handlers' fields (F-18: email's to/cc/bcc/subject/body,
			// webhook's url/headers/body), the download_artifact / extract
			// handlers' fields (F-21.6: they routinely name files produced
			// by PRIOR steps, so they render against the step's condition
			// context), the probe handler's fields (F-22: a probed URL may
			// cite a prior step's output, e.g. a dynamically allocated
			// ingress host), the rollback handler's fields (F-22: the
			// cut point may cite a run parameter), and publish_artifact's
			// patterns (F-21.6: glob patterns contain "[" bracket
			// expressions that must not be mistaken for template actions)
			// are rendered lazily; they are not rendered here.
			// publish_artifact's root/fileName stay eager (F-10
			// run-parameter interpolation).
			if stepType == TypeEmail || stepType == TypeWebhook || stepType == TypeDownloadArtifact ||
				stepType == TypeExtract || stepType == TypeProbe || stepType == TypeRollback ||
				(stepType == TypePublishArtifact && k == "patterns") ||
				(stepType == TypeApproval && k == "message") {
				params[k] = v
				continue
			}
			rendered, err := interpolateParamValue(v, data)
			if err != nil {
				return nil, fmt.Errorf("param %q: %w", k, err)
			}
			params[k] = rendered
		}
		clone.Params = params
	}
	return clone, nil
}

// interpolateParamValue returns a copy of value with its string and string
// list values rendered as Go templates against data (F-10). A nil value is
// returned unchanged.
func interpolateParamValue(value *dbpb.ParamValue, data map[string]any) (*dbpb.ParamValue, error) {
	if value == nil {
		return nil, nil
	}
	rendered := &dbpb.ParamValue{}
	if value.GetString_() != "" {
		s, err := renderTemplate(value.GetString_(), data)
		if err != nil {
			return nil, err
		}
		rendered.String_ = s
	}
	if len(value.GetStrings()) > 0 {
		strs := make([]string, len(value.GetStrings()))
		for i, s := range value.GetStrings() {
			if s == "" {
				strs[i] = s
				continue
			}
			r, err := renderTemplate(s, data)
			if err != nil {
				return nil, err
			}
			strs[i] = r
		}
		rendered.Strings = strs
	}
	return rendered, nil
}

// interpolateData builds the data a spec field is interpolated against (F-10):
// the run's parameter values (top-level, so `{{ .name }}` works) plus the
// structured keys "params" (the run's parameter values), "secrets" (the
// pipeline's named secrets as plaintext, F-12), and "run" (the run's
// identity). The structured keys are set after the params so they win over any
// same-named parameter. Secrets are only reachable under the "secrets" key
// (`{{ .secrets.name }}`), never top-level, so a secret can never collide with
// a parameter of the same name.
func interpolateData(run RunInfo) map[string]any {
	params := run.Params
	if params == nil {
		params = map[string]string{}
	}
	secrets := run.Secrets
	if secrets == nil {
		secrets = map[string]string{}
	}
	data := make(map[string]any, len(params)+3)
	for k, v := range params {
		data[k] = v
	}
	data["params"] = params
	data["secrets"] = secrets
	data["run"] = run
	return data
}

// renderTemplate renders s (a Go template, F-10) against data and returns the
// rendered string. It uses missingkey=error, so a reference to a parameter
// with no value (no supplied value and no default) fails rather than silently
// rendering to an empty string — an undefined reference is a spec error. A
// template that fails to parse or render is likewise a spec error.
func renderTemplate(s string, data map[string]any) (string, error) {
	tmpl, err := template.New("interpolation").Option("missingkey=error").Parse(s)
	if err != nil {
		return "", fmt.Errorf("parse template %q: %w", s, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render template %q: %w", s, err)
	}
	return buf.String(), nil
}

// RenderTemplate renders s (a Go template) against data and returns the
// rendered string. It is the public form of renderTemplate, exposed so a step
// handler (the approval handler, F-13) can render a template — such as an
// approval message — against the step's condition context (secrets, prior
// steps' outputs, run parameters, upstream jobs, and the job's identity). It
// uses the same missingkey=error semantics as spec interpolation (F-10) and a
// step's condition (F-06): a reference to a key that is absent from data is a
// render error.
func RenderTemplate(s string, data map[string]any) (string, error) {
	return renderTemplate(s, data)
}

// readStepOutputs reads every file in dir (the step's output directory, F-06)
// and records each file's (trimmed) contents as an output keyed by its file
// name. A step produces an output simply by writing a file into its output
// directory; it does not have to declare its outputs. Only regular files are
// read (subdirectories are ignored). It returns nil when dir is empty or
// contains no files.
func readStepOutputs(dir string) map[string]string {
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out map[string]string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[entry.Name()] = strings.TrimSpace(string(data))
	}
	return out
}

// ---------------------------------------------------------------------------
// Step results (F-06)
// ---------------------------------------------------------------------------

// StepStatus is the terminal outcome of a single step within a job's
// execution spec. A step does not have its own pending or cancelled state
// (those belong to the job as a whole) — only the states a step can actually
// reach once Execute has decided its fate.
type StepStatus string

const (
	StepStatusSucceeded StepStatus = "succeeded"
	StepStatusFailed    StepStatus = "failed"
	// StepStatusSkipped means the step's Condition rendered to false, so the
	// step never ran; it does not fail the job.
	StepStatusSkipped  StepStatus = "skipped"
	StepStatusTimedOut StepStatus = "timed_out"
)

// StepResult is one step's terminal outcome, as collected by
// StepResultCollector.
type StepResult struct {
	Name   string
	Status StepStatus
	// Error is a descriptive error message; empty when Status is Succeeded or
	// Skipped.
	Error string
	// Outputs are the named values the step produced (F-06, each file it wrote
	// into its per-step output directory, keyed by file name); empty when the
	// step wrote no output files or did not run.
	Outputs map[string]string
}

// StepStatusReporter receives each step's terminal status (and outputs) as
// Execute runs the spec (F-06), so a caller can persist per-step outcomes —
// including a step skipped by its condition — alongside the job's own final
// status. Set one in the context via ContextWithStepStatusReporter; when
// absent, step outcomes are simply not collected (Execute's returned error
// remains the authoritative job outcome).
//
// Implementations must be safe for concurrent use.
type StepStatusReporter interface {
	// ReportStepStatus records stepName's terminal status. errMsg is a
	// descriptive error message, empty when status is StepStatusSucceeded or
	// StepStatusSkipped; outputs are the named values the step produced
	// (nil/empty when it wrote no output files or did not run).
	ReportStepStatus(stepName string, status StepStatus, errMsg string, outputs map[string]string)
}

type stepStatusReporterKey struct{}

// ContextWithStepStatusReporter returns a context that carries reporter, so
// Execute reports each step's terminal status to it. A nil reporter returns
// ctx unchanged.
func ContextWithStepStatusReporter(ctx context.Context, reporter StepStatusReporter) context.Context {
	if reporter == nil {
		return ctx
	}
	return context.WithValue(ctx, stepStatusReporterKey{}, reporter)
}

// StepStatusReporterFromContext returns the StepStatusReporter carried by
// ctx, or nil when none is set.
func StepStatusReporterFromContext(ctx context.Context) StepStatusReporter {
	reporter, _ := ctx.Value(stepStatusReporterKey{}).(StepStatusReporter)
	return reporter
}

// reportStepStatus reports a step's terminal status to the context's
// StepStatusReporter, when one is set.
func reportStepStatus(ctx context.Context, stepName string, status StepStatus, errMsg string, outputs map[string]string) {
	if reporter := StepStatusReporterFromContext(ctx); reporter != nil {
		reporter.ReportStepStatus(stepName, status, errMsg, outputs)
	}
}

// StepObserver is an optional observer of each step's execution (F-20), set
// in the context via ContextWithStepObserver. Unlike the StepStatusReporter
// (which receives only a step's terminal status), an observer is told when a
// step starts and when it finishes, so a caller (the worker or agent) can
// record per-step metrics — the step's type and its duration — without the
// executor knowing about metrics. When absent, steps are simply not observed.
//
// Implementations must be safe for concurrent use.
type StepObserver interface {
	// StepStarted is called just before a step runs, with the step's type
	// (an empty type is dispatched to DefaultType) and its name.
	StepStarted(stepType string, stepName string)
	// StepFinished is called when a step reaches a terminal status, with the
	// step's type, its terminal status, and how long it ran (zero for a step
	// that was skipped by its condition or by another target's once claim).
	StepFinished(stepType string, status StepStatus, d time.Duration)
}

type stepObserverKey struct{}

// ContextWithStepObserver returns a context that carries observer, so Execute
// observes each step's start and finish. A nil observer returns ctx unchanged.
func ContextWithStepObserver(ctx context.Context, observer StepObserver) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, stepObserverKey{}, observer)
}

// StepObserverFromContext returns the StepObserver carried by ctx, or nil when
// none is set.
func StepObserverFromContext(ctx context.Context) StepObserver {
	observer, _ := ctx.Value(stepObserverKey{}).(StepObserver)
	return observer
}

// observeStep calls observer.StepStarted for step, when an observer is set.
func observeStep(ctx context.Context, step *dbpb.JobStep, stepName string) {
	if observer := StepObserverFromContext(ctx); observer != nil {
		observer.StepStarted(stepTypeOf(step), stepName)
	}
}

// finishStep calls observer.StepFinished for step's terminal status, when an
// observer is set.
func finishStep(ctx context.Context, step *dbpb.JobStep, status StepStatus, d time.Duration) {
	if observer := StepObserverFromContext(ctx); observer != nil {
		observer.StepFinished(stepTypeOf(step), status, d)
	}
}

// stepTypeOf returns the step's type, or DefaultType when it is empty (the
// type a step is dispatched to).
func stepTypeOf(step *dbpb.JobStep) string {
	if t := step.GetType(); t != "" {
		return t
	}
	return DefaultType
}

// StepResultCollector is a StepStatusReporter that accumulates each step's
// terminal status (and outputs) in the order Execute reports it, for a caller
// (the worker or agent) to attach to its final job status report (F-06). It
// also aggregates the job's outputs: the union of its steps' outputs, with a
// later step overriding an earlier one on a name collision.
type StepResultCollector struct {
	mu         sync.Mutex
	Results    []StepResult
	jobOutputs map[string]string
}

// ReportStepStatus implements StepStatusReporter.
func (c *StepResultCollector) ReportStepStatus(stepName string, status StepStatus, errMsg string, outputs map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Results = append(c.Results, StepResult{Name: stepName, Status: status, Error: errMsg, Outputs: outputs})
	for k, v := range outputs {
		if c.jobOutputs == nil {
			c.jobOutputs = make(map[string]string)
		}
		c.jobOutputs[k] = v
	}
}

// All returns a snapshot of the collected step results, in the order they
// were reported.
func (c *StepResultCollector) All() []StepResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	results := make([]StepResult, len(c.Results))
	copy(results, c.Results)
	return results
}

// JobOutputs returns the job's aggregated outputs (the union of its steps'
// outputs, a later step overriding an earlier one on a name collision), or
// nil when no step produced any.
func (c *StepResultCollector) JobOutputs() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.jobOutputs) == 0 {
		return nil
	}
	out := make(map[string]string, len(c.jobOutputs))
	for k, v := range c.jobOutputs {
		out[k] = v
	}
	return out
}

// stepStatusToProto converts a StepStatus to its proto enum value.
func stepStatusToProto(status StepStatus) dbpb.StepStatus {
	switch status {
	case StepStatusSucceeded:
		return dbpb.StepStatus_STEP_STATUS_SUCCEEDED
	case StepStatusFailed:
		return dbpb.StepStatus_STEP_STATUS_FAILED
	case StepStatusSkipped:
		return dbpb.StepStatus_STEP_STATUS_SKIPPED
	case StepStatusTimedOut:
		return dbpb.StepStatus_STEP_STATUS_TIMED_OUT
	default:
		return dbpb.StepStatus_STEP_STATUS_UNSPECIFIED
	}
}

// ToProto returns the collected step results as their proto representation,
// ready to attach to a ReportJobStatusRequest (F-06).
func (c *StepResultCollector) ToProto() []*dbpb.StepResult {
	results := c.All()
	if len(results) == 0 {
		return nil
	}
	proto := make([]*dbpb.StepResult, len(results))
	for i, result := range results {
		proto[i] = &dbpb.StepResult{
			Name:    result.Name,
			Status:  stepStatusToProto(result.Status),
			Error:   result.Error,
			Outputs: result.Outputs,
		}
	}
	return proto
}

// stepTypes maps a step type to its handler. Handlers register themselves via
// RegisterStepType (the built-in shell handler does so at package init in
// internal/stephandlers); targets and plugins add more the same way.
var stepTypes sync.Map // map[string]StepHandler

// RegisterStepType registers a handler for a step type so steps of that type
// can be executed. The built-in "shell" handler is always registered; this is
// how a target (or a plugin) adds new step types such as "ansible",
// "terraform", or "argo". Registering a type that is already registered
// replaces its handler.
func RegisterStepType(name string, handler StepHandler) {
	if name == "" || handler == nil {
		return
	}
	stepTypes.Store(name, handler)
}

// runStep executes a single step by dispatching to the handler registered for
// the step's type (the built-in "shell" handler when the type is empty). It
// returns a descriptive error when the step fails or its type is not
// registered on this target.
//
// When the step's context expires (a per-step timeout, or the job-level
// timeout inherited from the parent context), the error is wrapped with
// ErrTimeout so the caller can report the job as timed_out. This is checked
// here (rather than relying on each handler to do it) so that any step
// handler — not just the shell handler — has its step-level timeout detected.
func runStep(ctx context.Context, stepName string, step *dbpb.JobStep, logger *slog.Logger) error {
	name := step.GetType()
	if name == "" {
		name = DefaultType
	}
	handler, ok := stepTypes.Load(name)
	if !ok {
		return fmt.Errorf("executor: step %q: unknown step type %q (no handler registered on this target)", stepName, name)
	}

	// A per-step timeout derives a context that expires after the step's
	// timeout. A zero timeout means no per-step limit; the parent ctx (which
	// may itself carry the job-level timeout) still bounds the step.
	stepCtx := ctx
	if timeout := step.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// Propagate the step's name so a step handler that captures output can
	// attribute it to the right step (for the LogSink).
	stepCtx = context.WithValue(stepCtx, stepNameKey{}, stepName)

	if err := handler.(StepHandler)(stepCtx, step, logger); err != nil {
		err = fmt.Errorf("executor: step %q (type %q): %w", stepName, name, err)
		// A deadline (the step's own timeout, or the job-level timeout
		// inherited from the parent context) expired while the step ran: mark
		// it as a timeout so the job is reported as timed_out.
		if errors.Is(stepCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %w", ErrTimeout, err)
		}
		return err
	}
	return nil
}
