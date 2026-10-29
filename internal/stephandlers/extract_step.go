// extract_step.go — the built-in extract step handler (F-21.6): unpacks a
// tar / gzipped tar / zip archive into a destination directory (default: the
// job's staged working area).
package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeExtract is the step type of the built-in archive-extraction handler
// (F-21.6).
const TypeExtract = executor.TypeExtract

// Param keys the extract handler reads from a step's Params map.
const (
	ParamArchive        = "archive"
	ParamStripComponents = "stripComponents"
)

func init() {
	executor.RegisterStepType(TypeExtract, runExtractStep)
}

// runExtractStep is the built-in "extract" step handler (F-21.6). It unpacks
// the archive named by "archive" (lazy-rendered against the step's condition
// context, so it can reference a prior step's output — typically the
// "path" output of a download_artifact step) into "dest" (default: the job's
// staged working area; relative dests resolve against the stage area,
// absolutes are honored). The format is detected from the archive's magic
// bytes (gzipped tar, zip, or plain tar — file names are not trusted);
// "stripComponents" drops leading path components from every member (GNU tar
// --strip-components semantics).
//
// Member names are treated hostilely: escaping paths and link-typed members
// are refused (the step fails with "refusing member <name>"); conflicts with
// existing files are overwritten (last writer wins).
//
// Records a "dest" step output (the absolute extraction directory).
func runExtractStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	condCtx := executor.ConditionContextFromContext(ctx)
	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

	archive, err := renderField(step, ParamArchive, condCtx)
	if err != nil {
		return err
	}
	if archive == "" {
		return fmt.Errorf("archive is required")
	}
	dest, err := renderField(step, ParamDest, condCtx)
	if err != nil {
		return err
	}
	stripRaw := paramString(step, ParamStripComponents)
	strip, err := renderIntParam(ParamStripComponents, stripRaw, condCtx, 0)
	if err != nil {
		return err
	}
	if strip < 0 {
		return fmt.Errorf("stripComponents must be >= 0")
	}

	stageDir := stageDirFromStep(step)
	destAbs := dest
	if destAbs == "" {
		if stageDir == "" {
			return fmt.Errorf("dest is required (no staged working area is provisioned for this job)")
		}
		destAbs = stageDir
	} else if !filepath.IsAbs(destAbs) {
		if stageDir == "" {
			return fmt.Errorf("dest %q is relative but no staged working area is provisioned for this job", dest)
		}
		destAbs = filepath.Join(stageDir, destAbs)
	}

	stats, err := ExtractArchive(ctx, archive, stageDir, destAbs, extractOptions{StripComponents: strip})
	if err != nil {
		return err
	}

	if sink != nil {
		msg := fmt.Sprintf("extract: archive=%s format=%s dest=%s members=%d", archive, stats.Format, stats.Dest, stats.Members)
		if stats.Stripped > 0 {
			msg += fmt.Sprintf("[ stripped=%d]", stats.Stripped)
		}
		sink.WriteStepOutput(stepName, "stdout", []byte(msg+"\n"))
	}
	logger.Info("executor: extracted archive", "archive", archive, "format", stats.Format, "dest", stats.Dest, "members", stats.Members, "stripped", stats.Stripped)
	return writeStepOutput(step, "dest", stats.Dest)
}
