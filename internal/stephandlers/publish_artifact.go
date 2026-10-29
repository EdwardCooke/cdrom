// publish_artifact.go — the built-in publish_artifact step handler (F-21.6):
// collects the files matched by multiline glob patterns (with "-"-negated
// exclusions) into a zip that preserves the matched directory structure, and
// uploads it to the job's OWN artifact namespace (job-bound by construction —
// the job token's namespace scope).
package stephandlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypePublishArtifact is the step type of the built-in artifact-publishing
// handler (F-21.6).
const TypePublishArtifact = executor.TypePublishArtifact

// Param keys the publish_artifact handler reads from a step's Params map.
const (
	ParamPatterns = "patterns"
	ParamRoot     = "root"
	ParamFileName = "fileName"
)

// defaultPublishFileName is the zip's base name when the step sets no
// "fileName" param.
const defaultPublishFileName = "publish.zip"

func init() {
	executor.RegisterStepType(TypePublishArtifact, runPublishArtifactStep)
}

// runPublishArtifactStep is the built-in "publish_artifact" step handler
// (F-21.6). It:
//
//  1. Parses the "patterns" param (lazy-rendered against the step's
//     condition context; one glob per line, "-"-prefixed lines negate, blank
//     lines and "#" comments ignored) and collects the SURVIVING files under
//     "root" (default: the job's staged working area; relative roots resolve
//     against it, absolutes are honored). Matching uses the vendored
//     double-star globber (GlobX): "**" spans zero-or-more directory levels;
//     on Windows "/" and "\" are both path separators.
//  2. Sanitizes the matched files' entry names for Windows compatibility
//     (forbidden characters and reserved device basenames remapped to "_")
//     and assigns collision-free names (counter suffix before the extension;
//     the uncontested name goes to the file that needed no rewriting).
//  3. Zips them (structure preserved relative to root, entries sorted,
//     mtimes kept) into the stage area under a UNIQUE name (random suffix —
//     fan-out workers share the OS temp dir and must never contend), and
//  4. Streams the zip to the job's own (job-id) artifact namespace through
//     the execution target's ArtifactTransfer.
//
// An empty match set fails the step ("no files matched the given patterns") —
// a typo in a glob must not quietly publish an empty package.
//
// Records a "path" step output (the zip's absolute location on the target)
// and an "artifact" step output (the store-relative name the zip was
// uploaded under), so later steps and the condition context can address it.
func runPublishArtifactStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	transfer := executor.ArtifactTransferFromContext(ctx)
	if transfer == nil {
		return fmt.Errorf("artifact transfers are not supported by this execution target")
	}
	condCtx := executor.ConditionContextFromContext(ctx)
	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

	patternsRaw, err := renderField(step, ParamPatterns, condCtx)
	if err != nil {
		return err
	}
	if patternsRaw == "" {
		return fmt.Errorf("patterns is required")
	}
	pats, err := ParsePackPatterns(patternsRaw)
	if err != nil {
		return err
	}
	rootRaw, err := renderField(step, ParamRoot, condCtx)
	if err != nil {
		return err
	}
	fileNameRaw, err := renderField(step, ParamFileName, condCtx)
	if err != nil {
		return err
	}
	if fileNameRaw == "" {
		fileNameRaw = defaultPublishFileName
	}

	stageDir := stageDirFromStep(step)
	root := rootRaw
	if root == "" {
		if stageDir == "" {
			return fmt.Errorf("root is required (no staged working area is provisioned for this job)")
		}
		root = stageDir
	} else if !filepath.IsAbs(root) {
		if stageDir == "" {
			return fmt.Errorf("root %q is relative but no staged working area is provisioned for this job", root)
		}
		root = filepath.Join(stageDir, root)
	}

	stats, entries, err := PackCollect(ctx, root, pats)
	if err != nil {
		return err
	}

	// Assemble the zip in the stage area under a UNIQUE name (a random
	// suffix): fan-out workers of the same job share the OS temp dir, and a
	// deterministic name would make concurrent attempts contend.
	host := stageDir
	if host == "" {
		host, err = os.MkdirTemp("", "cdrom-stage-")
		if err != nil {
			return fmt.Errorf("create scratch dir: %w", err)
		}
		defer os.RemoveAll(host)
	}
	sfx, err := randHex(6)
	if err != nil {
		return err
	}
	zipName := trimExt(fileNameRaw) + "-" + sfx + ".zip"
	zipDest := filepath.Join(host, zipName)
	if err := PackZip(ctx, zipDest, entries); err != nil {
		os.Remove(zipDest)
		return fmt.Errorf("assemble zip: %w", err)
	}

	// Upload into the job's OWN namespace (the job-id): the job token's
	// scope permits exactly this, so the package is bound to the producing
	// job by construction.
	job := executor.JobIdentityFromContext(ctx)
	namespace := strconv.FormatInt(job.ID, 10)
	storeName := fileNameRaw
	if sz, err := os.Stat(zipDest); err == nil {
		stats.ZipSize = sz.Size()
	}
	bytesUploaded, err := transfer.UploadArtifact(ctx, namespace, storeName, zipDest)
	if err != nil {
		os.Remove(zipDest)
		return fmt.Errorf("upload artifact %q: %w", storeName, err)
	}
	// The LOCAL zip is KEPT (until the job's stage area is cleaned up at
	// terminus): the "path" step output advertises it, and a later step of the
	// same job (an extract, a shell installer) can consume it without a
	// second download.

	if sink != nil {
		msg := fmt.Sprintf("publish_artifact: root=%s matched=%d excluded=%d zip=%s size=%d uploaded=%s",
			stats.Root, stats.Matched, stats.Excluded, zipDest, bytesUploaded, storeName)
		if stats.Sanitized > 0 {
			msg += fmt.Sprintf(" sanitized=%d", stats.Sanitized)
		}
		if stats.Renamed > 0 {
			msg += fmt.Sprintf(" renamed=%d", stats.Renamed)
		}
		sink.WriteStepOutput(stepName, "stdout", []byte(msg+"\n"))
	}
	logger.Info("executor: published artifact", "root", stats.Root, "matched", stats.Matched, "excluded", stats.Excluded,
		"sanitized", stats.Sanitized, "renamed", stats.Renamed, "namespace", namespace, "name", storeName, "size", bytesUploaded)
	if err := writeStepOutput(step, "path", zipDest); err != nil {
		return err
	}
	return writeStepOutput(step, "artifact", storeName)
}

// randHex returns n random bytes as lowercase hex.
func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// trimExt drops the final extension from a file name ("a.b.zip" → "a.b").
func trimExt(name string) string {
	base := filepath.Base(name)
	ext := filepath.Ext(base)
	if ext == "" {
		return base
	}
	return base[:len(base)-len(ext)]
}
