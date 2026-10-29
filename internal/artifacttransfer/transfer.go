// Package artifacttransfer implements the executor's ArtifactTransfer seam
// (F-21.6) for execution targets: it moves files to and from the artifacts
// store THROUGH THE API (the topology rule — targets talk only to the
// API), presenting the job's own token. The API's artifact-proxy RPCs
// (DownloadArtifact / UploadArtifact) forward the traffic to the artifacts
// service and enforce the job-token's namespace scope.
//
// Namespace addressing: a namespace is either a RAW store namespace (the
// job's own id, for uploads) or a logical "run:<id>" pseudo-namespace (for
// downloads); the API substitutes the run's store namespace for the latter.
// Targets never learn the run's namespace themselves.
//
// Downloads are crash-safe: the file is written to a dot-prefixed temp name
// beside its destination and renamed into place only on success, so a failed
// or cancelled download leaves no partial file behind.
package artifacttransfer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	apipb "cdrom/internal/gen/cdrom/api/v1"
	"cdrom/internal/grpcutil"
)

// chunkSize is the transfer buffer size (downloads and uploads alike).
const chunkSize = 256 << 10

// Transfer moves files to/from the artifacts store through the API.
type Transfer struct {
	api   apipb.APIClient
	jobID int64
	token string
}

// New builds a Transfer for jobID presenting token on every API call.
func New(api apipb.APIClient, jobID int64, token string) *Transfer {
	return &Transfer{api: api, jobID: jobID, token: token}
}

// DownloadArtifact copies the file named name in namespace onto the target
// at dest (parents created; an existing file is replaced). It returns the
// number of bytes transferred.
func (t *Transfer) DownloadArtifact(ctx context.Context, namespace, name, dest string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, fmt.Errorf("create dest parent: %w", err)
	}
	// Hidden temp name beside the destination: a failed/cancelled download
	// leaves no partial file (and the dot prefix keeps it out of the way of
	// globbers scanning the stage area).
	tmp := dest + ".~dl"
	defer os.Remove(tmp) // a no-op once the rename succeeds

	stream, err := t.api.DownloadArtifact(t.auth(ctx), &apipb.LogFileRef{
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return 0, fmt.Errorf("open download stream: %w", err)
	}
	out, err := os.Create(tmp)
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	var written int64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			out.Close()
			return 0, fmt.Errorf("receive chunk: %w", err)
		}
		n, werr := out.Write(chunk.GetData())
		if werr != nil {
			out.Close()
			return 0, fmt.Errorf("write chunk: %w", werr)
		}
		written += int64(n)
	}
	if err := out.Close(); err != nil {
		return 0, fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return 0, fmt.Errorf("move into place: %w", err)
	}
	return written, nil
}

// UploadArtifact streams the file at src into the store as name in
// namespace. It returns the number of bytes transferred.
func (t *Transfer) UploadArtifact(ctx context.Context, namespace, name, src string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	stream, err := t.api.UploadArtifact(t.auth(ctx))
	if err != nil {
		return 0, fmt.Errorf("open upload stream: %w", err)
	}
	// The first chunk carries the metadata (namespace and name).
	if err := stream.Send(&apipb.LogFileChunk{
		Metadata: &apipb.LogFile{Namespace: namespace, Name: name},
		Data:     make([]byte, 0),
	}); err != nil {
		return 0, fmt.Errorf("send metadata: %w", err)
	}
	var uploaded int64
	buf := make([]byte, chunkSize)
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if err := stream.Send(&apipb.LogFileChunk{Data: buf[:n]}); err != nil {
				return 0, fmt.Errorf("send chunk: %w", err)
			}
			uploaded += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return 0, fmt.Errorf("read source file: %w", rerr)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return 0, fmt.Errorf("finish upload: %w", err)
	}
	return uploaded, nil
}

// GetArtifact reports whether the file named name exists in namespace (a
// metadata round trip through the API's artifact proxy). The API enforces the
// job-token's namespace scope on the call.
func (t *Transfer) GetArtifact(ctx context.Context, namespace, name string) error {
	_, err := t.api.GetArtifact(t.auth(ctx), &apipb.LogFileRef{
		Namespace: namespace,
		Name:      name,
	})
	return err
}

// auth decorates ctx with the job's bearer token (the API verifies it is
// scoped appropriately for the namespace addressed).
func (t *Transfer) auth(ctx context.Context) context.Context {
	if t.token == "" {
		return ctx
	}
	return grpcutil.WithBearerToken(ctx, t.token)
}
