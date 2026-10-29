// artifact_helpers.go — shared plumbing for the F-21.6 artifact step
// handlers (download_artifact, extract, publish_artifact).
package stephandlers

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// stageDirFromStep returns the job's staged working area as exported to the
// step (executor.StageAreaEnv, set by the executor when the target provisions
// a stage area), or "" when the job has none.
func stageDirFromStep(step *dbpb.JobStep) string {
	return step.GetEnv()[executor.StageAreaEnv]
}

// writeStepOutput writes value into the step's per-step output directory
// under name (the executor reads every file there back as a step output,
// keyed by file name — F-01/F-06). A step with no output directory (unit
// tests) is a no-op.
func writeStepOutput(step *dbpb.JobStep, name, value string) error {
	dir := step.GetEnv()[executor.StepOutputDirEnv]
	if dir == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
		return fmt.Errorf("write step output %q: %w", name, err)
	}
	return nil
}

// renderIntParam renders the integer-valued param named key against the
// condition context (lazy-rendered params are Go templates, F-10) and parses
// it. Absent/empty yields def. A value that fails to render or parse is a
// spec error.
func renderIntParam(key, raw string, condCtx map[string]any, def int) (int, error) {
	if raw == "" {
		return def, nil
	}
	rendered, err := executor.RenderTemplate(raw, condCtx)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	v, err := strconv.Atoi(rendered)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not an integer", key, rendered)
	}
	return v, nil
}
