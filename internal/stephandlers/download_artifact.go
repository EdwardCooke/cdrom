// download_artifact.go — the built-in download_artifact step handler
// (F-21.6): copies a named artifact from the run's store namespace onto the
// target, into the job's staged working area (or an explicit dest).
package stephandlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"cdrom/internal/executor"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// TypeDownloadArtifact is the step type of the built-in artifact-download
// handler (F-21.6).
const TypeDownloadArtifact = executor.TypeDownloadArtifact

// Param keys the download_artifact handler reads from a step's Params map.
const (
	ParamArtifactRef = "artifact"
	ParamDest        = "dest"
)

func init() {
	executor.RegisterStepType(TypeDownloadArtifact, runDownloadArtifactStep)
}

// runDownloadArtifactStep is the built-in "download_artifact" step handler
// (F-21.6). It resolves the step's "artifact" param (lazy-rendered against
// the step's condition context, so it can name a file a PRIOR step produced —
// e.g. the output of a publish_artifact step) to a concrete store file and
// copies it onto the target through the execution target's ArtifactTransfer:
//
//   - Resolution: (1) if the rendered name matches a spec.artifacts entry of
//     THIS run (the run's resolved-artifact snapshot, carried on the job and
//     exposed via the executor's RunInfo), that entry's PINNED registry
//     filing name is used (a re-kicked run always gets the version it
//     originally resolved — the snapshot is the per-run freeze); (2)
//     otherwise the name is taken literally as a file in the job's OWN
//     (job-id) namespace (the job's own publications); (3) otherwise as a
//     file in the package REGISTRY namespace (public packages — the everyday
//     "grab the published build" path).
//   - Destination: the "dest" param (lazy-rendered; relative → the job's
//     staged working area, absolute honored; parents created). Default: the
//     stage area root, under the artifact's stored file name.
//
// Records a "path" step output (the absolute location the file landed at) so
// later steps' conditions/templates can address it.
func runDownloadArtifactStep(ctx context.Context, step *dbpb.JobStep, logger *slog.Logger) error {
	transfer := executor.ArtifactTransferFromContext(ctx)
	if transfer == nil {
		return fmt.Errorf("artifact transfers are not supported by this execution target")
	}
	condCtx := executor.ConditionContextFromContext(ctx)
	sink := executor.LogSinkFromContext(ctx)
	stepName := executor.StepNameFromContext(ctx)

	ref, err := renderField(step, ParamArtifactRef, condCtx)
	if err != nil {
		return err
	}
	if ref == "" {
		return fmt.Errorf("artifact is required")
	}
	dest, err := renderField(step, ParamDest, condCtx)
	if err != nil {
		return err
	}

	locations := resolveArtifactLocations(ctx, ref)

	// Probe the candidates in preference order; the first that EXISTS is the
	// one downloaded. (Existence probing is cheap — a metadata round trip —
	// and makes the resolution ORDER observable in the failure diagnostic.)
	var chosen [2]string
	found := false
	for _, loc := range locations {
		if err := transfer.GetArtifact(ctx, loc[0], loc[1]); err == nil {
			chosen = loc
			found = true
			break
		}
	}
	if !found {
		var probed []string
		for _, loc := range locations {
			probed = append(probed, loc[0]+"/"+loc[1])
		}
		return fmt.Errorf("no such artifact %q (probed: %s)", ref, strings.Join(probed, ", "))
	}
	namespace, fileName := chosen[0], chosen[1]

	stageDir := stageDirFromStep(step)
	destAbs := dest
	if destAbs == "" {
		if stageDir == "" {
			return fmt.Errorf("dest is required (no staged working area is provisioned for this job)")
		}
		destAbs = filepath.Join(stageDir, fileName)
	} else if !filepath.IsAbs(destAbs) {
		if stageDir == "" {
			return fmt.Errorf("dest %q is relative but no staged working area is provisioned for this job", dest)
		}
		destAbs = filepath.Join(stageDir, destAbs)
	}
	if err := os.MkdirAll(filepath.Dir(destAbs), 0o755); err != nil {
		return fmt.Errorf("create dest parent: %w", err)
	}

	size, err := transfer.DownloadArtifact(ctx, namespace, fileName, destAbs)
	if err != nil {
		// A failed download leaves no partial file (the transfer writes to a
		// hidden temp name and renames on success).
		return fmt.Errorf("download artifact %q from namespace %q: %w", fileName, namespace, err)
	}

	if sink != nil {
		msg := fmt.Sprintf("download_artifact: artifact=%s file=%s dest=%s size=%d\n", ref, fileName, destAbs, size)
		sink.WriteStepOutput(stepName, "stdout", []byte(msg))
	}
	logger.Info("executor: downloaded artifact", "artifact", ref, "file", fileName, "namespace", namespace, "dest", destAbs, "size", size)
	return writeStepOutput(step, "path", destAbs)
}

// resolveArtifactLocations builds the CANDIDATE (namespace, file name) list
// for a (rendered) artifact reference, in preference order (F-21.6,
// storage-redesign era):
//
//  1. the run's resolved-artifact snapshot: a reference matching a
//     spec.artifacts entry of the run resolves to that entry's PINNED
//     registry filing name (the per-run freeze — a re-kicked run always gets
//     the version it originally resolved);
//  2. the literal name in the job's OWN (job-id) namespace (the job's own
//     published files — a run job's own publications, or a standalone
//     job's);
//  3. the literal name in the package REGISTRY namespace (public packages —
//     the everyday "grab the published build" path for standalone jobs).
//
// The handler probes the candidates in order (GetArtifact) and downloads the
// first that exists; a reference that matches none fails with a diagnostic
// naming every probed location.
func resolveArtifactLocations(ctx context.Context, ref string) [][2]string {
	var out [][2]string
	run := executor.RunInfoFromContext(ctx)
	if run.ID != 0 {
		if stored, ok := run.Artifacts[ref]; ok && stored != "" {
			out = append(out, [2]string{registryNamespace, stored})
		}
	}
	job := executor.JobIdentityFromContext(ctx)
	out = append(out, [2]string{strconv.FormatInt(job.ID, 10), ref})
	out = append(out, [2]string{registryNamespace, ref})
	return out
}

// registryNamespace is the reserved package-registry store namespace (F-21.6)
// — the same constant the database service and the API's publish endpoint
// use (mirrored here to keep the handler dependency-light).
const registryNamespace = "packages"
