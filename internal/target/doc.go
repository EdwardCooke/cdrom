// Package target is the shared execution layer for the two kinds of
// execution targets — the long-lived worker and the ephemeral agent. Both
// talk only to the API and run a job's spec through the shared executor, so
// the parts they have in common live here rather than being duplicated in
// each target:
//
//   - building the executor's run context (the log sink that streams a step's
//     output to the API, the step-status reporter that gathers each step's
//     terminal outcome, the upstream jobs and the job's identity for a step's
//     condition, the run's parameters and identity for spec interpolation,
//     and the token exchanger a step handler uses to request a job token for a
//     different audience); and
//   - reporting a job's final status (with its collected step results and
//     outputs) back to the API, presenting the job's token.
//
// The two targets differ only in how they acquire a job (a worker holds a
// WatchJobs stream and polls; an agent is spawned with a job id) and in the
// worker-only cross-worker step barrier, which a target supplies through
// Context.StepBarrier. The package is shared by the worker and the agent and
// must run on both Windows and Linux.
package target
