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
	// The condition context is built up as the job runs: priorSteps accumulates
	// each step's status/outputs, while the upstream jobs and the job's
	// identity come from the context (set by the execution target, F-06).
	upstreamJobs := UpstreamJobsFromContext(ctx)
	identity := JobIdentityFromContext(ctx)
	priorSteps := make([]StepConditionInfo, 0, len(spec.GetSteps()))
	for i, step := range spec.GetSteps() {
		// A step's condition (F-06) is evaluated before the step runs: when it
		// renders to false the step is skipped (it does not run and does not
		// fail the job); when it fails to parse or render, that is a spec
		// error and fails the job.
		condCtx := conditionContext(step, priorSteps, upstreamJobs, identity)
		skip, condErr := evaluateCondition(step, condCtx)
		if condErr != nil {
			err := fmt.Errorf("executor: step %d: condition: %w", i, condErr)
			reportStepStatus(ctx, i, StepStatusFailed, err.Error(), nil)
			return err
		}
		if skip {
			logger.Info("executor: step skipped (condition not met)", "step", i)
			reportStepStatus(ctx, i, StepStatusSkipped, "", nil)
			priorSteps = append(priorSteps, StepConditionInfo{Index: i, Status: string(StepStatusSkipped)})
			continue
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
			err := fmt.Errorf("executor: step %d: create output dir: %w", i, err)
			reportStepStatus(ctx, i, StepStatusFailed, err.Error(), nil)
			return err
		}
		stepToRun = withEnv(step, map[string]string{StepOutputDirEnv: outDir})
		stepErr := runStep(jobCtx, i, stepToRun, logger)
		stepStatus := StepStatusSucceeded
		if stepErr != nil {
			stepStatus = StepStatusFailed
			if errors.Is(stepErr, ErrTimeout) {
				stepStatus = StepStatusTimedOut
			}
		}
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
		reportStepStatus(ctx, i, stepStatus, errMsg, outputs)
		priorSteps = append(priorSteps, StepConditionInfo{Index: i, Status: string(stepStatus), Outputs: outputs})
		if stepErr != nil {
			// A failed/timed-out step stops the job unless it is ignore_failed
			// (F-06): the failure is recorded and the job continues.
			if step.GetIgnoreFailed() {
				logger.Info("executor: step failed but ignore_failed is set; continuing", "step", i)
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
			if err := barrier.SyncStep(jobCtx, i); err != nil {
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
	// WriteStepOutput records a chunk of a step's output. stepIndex is the
	// 0-based index of the step in the job's spec; stream is "stdout" or
	// "stderr" (see the JobLogStreamStdout/Stderr constants in internal/api).
	WriteStepOutput(stepIndex int, stream string, data []byte)
}

// Output stream names a LogSink reports for a chunk of step output.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

type logSinkKey struct{}

type stepIndexKey struct{}

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

// StepIndexFromContext returns the 0-based index of the step being executed,
// as set by runStep, or -1 when absent. Step handlers use it to attribute
// streamed output to the right step.
func StepIndexFromContext(ctx context.Context) int {
	index, _ := ctx.Value(stepIndexKey{}).(int)
	return index
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
	// SyncStep records that the worker completed stepIndex and blocks until the
	// barrier for that step is satisfied (every worker alive at the step's
	// start has completed it) or the job is cancelled (it returns ErrCancelled)
	// or ctx is done (it returns ctx.Err()).
	SyncStep(ctx context.Context, stepIndex int) error
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

// ---------------------------------------------------------------------------
// Condition context (F-06)
// ---------------------------------------------------------------------------

// StepConditionInfo is the condition-relevant view of a prior step of the
// same job: its index, terminal status, and the outputs it produced. A step's
// condition template can reference the prior steps via the "steps" key.
type StepConditionInfo struct {
	// Index is the 0-based index of the step in the job's spec.
	Index int
	// Status is the step's terminal status (succeeded, failed, skipped, or
	// timed_out).
	Status string
	// Outputs are the named values the step produced (each file it wrote into
	// its per-step output directory, keyed by file name); empty when the step
	// wrote no output files or did not run.
	Outputs map[string]string
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

// conditionContext builds the data a step's condition template is rendered
// against (F-06): the step's own env (so `{{ .NAME }}` still works as before),
// plus the structured keys "steps" (the prior steps' status/outputs), "jobs"
// (the upstream jobs' status/outputs), and "job" (the current job's
// identity). The structured keys are set after the env so they win over any
// same-named env var.
func conditionContext(step *dbpb.JobStep, priorSteps []StepConditionInfo, upstreamJobs []*dbpb.UpstreamJob, identity JobIdentity) map[string]any {
	data := make(map[string]any, len(step.GetEnv())+3)
	for k, v := range step.GetEnv() {
		data[k] = v
	}
	data["steps"] = priorSteps
	data["jobs"] = toUpstreamJobInfos(upstreamJobs)
	data["job"] = identity
	return data
}

// toUpstreamJobInfos converts the proto upstream jobs into their
// condition-relevant view (string status, so a condition can compare it
// directly). An empty list yields nil.
func toUpstreamJobInfos(jobs []*dbpb.UpstreamJob) []UpstreamJobInfo {
	if len(jobs) == 0 {
		return nil
	}
	out := make([]UpstreamJobInfo, len(jobs))
	for i, job := range jobs {
		out[i] = UpstreamJobInfo{
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
	Index  int
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
	// ReportStepStatus records stepIndex's terminal status. errMsg is a
	// descriptive error message, empty when status is StepStatusSucceeded or
	// StepStatusSkipped; outputs are the named values the step produced
	// (nil/empty when it wrote no output files or did not run).
	ReportStepStatus(stepIndex int, status StepStatus, errMsg string, outputs map[string]string)
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
func reportStepStatus(ctx context.Context, index int, status StepStatus, errMsg string, outputs map[string]string) {
	if reporter := StepStatusReporterFromContext(ctx); reporter != nil {
		reporter.ReportStepStatus(index, status, errMsg, outputs)
	}
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
func (c *StepResultCollector) ReportStepStatus(index int, status StepStatus, errMsg string, outputs map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Results = append(c.Results, StepResult{Index: index, Status: status, Error: errMsg, Outputs: outputs})
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
			Index:   int32(result.Index),
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
func runStep(ctx context.Context, index int, step *dbpb.JobStep, logger *slog.Logger) error {
	name := step.GetType()
	if name == "" {
		name = DefaultType
	}
	handler, ok := stepTypes.Load(name)
	if !ok {
		return fmt.Errorf("executor: step %d: unknown step type %q (no handler registered on this target)", index, name)
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
	// Propagate the step's index so a step handler that captures output can
	// attribute it to the right step (for the LogSink).
	stepCtx = context.WithValue(stepCtx, stepIndexKey{}, index)

	if err := handler.(StepHandler)(stepCtx, step, logger); err != nil {
		err = fmt.Errorf("executor: step %d (type %q): %w", index, name, err)
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
