// fileresolver.go — the API's (namespace, name) <-> storage_object bridge
// (F-21.6 storage redesign).
//
// Since the storage redesign the PHYSICAL store is a dumb blob service keyed
// by UUID, and all file ATTRIBUTES live in the Database service's
// storage_object records. This resolver is where the API performs the
// translation: every artifact/log interaction (the target-facing gRPC proxy,
// the HTTP job-artifact and job-log endpoints, the F-23 cross-pod log tail,
// the package registry surface) funnels through it.
//
// Simplicity over cleverness: reads (lookups, lists) go straight to the
// database — they are infrequent (per step / per UI poll, never per log
// CHUNK). The one hot path, a job's log stream, establishes each log's uid
// ONCE (ensureLog) into per-stream state and then appends by uid, so a burst
// of chunks causes no per-chunk database round trips. Mutations invalidate
// nothing because nothing is cached.
package api

import (
	"context"
	"mime"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// fileResolver translates (namespace, name) to storage_object records and
// drives the blob service by uid. Constructed ONCE per API process and shared
// by the HTTP and gRPC faces. A nil database client makes every operation
// fail with Unavailable (the blob service alone is not sufficient — records
// are the source of truth).
type fileResolver struct {
	db    dbpb.DatabaseClient
	blobs artifactspb.ArtifactsClient
}

// NewFileResolver wires a resolver over the API's database and blob clients.
// Constructed ONCE per API process and installed on BOTH the HTTP and gRPC
// faces (SetResolver), so the whole pod translates through one brain.
func NewFileResolver(db dbpb.DatabaseClient, blobs artifactspb.ArtifactsClient) *fileResolver {
	return &fileResolver{db: db, blobs: blobs}
}

// getFile returns the VISIBLE record for (namespace, name). A missing file is
// a NotFound status (callers map it to their surface's error shape).
func (r *fileResolver) getFile(ctx context.Context, namespace, name string) (*dbpb.StorageObject, error) {
	if r.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	return r.db.GetStorageObject(ctx, &dbpb.GetStorageObjectRequest{
		Namespace: namespace,
		Name:      name,
	})
}

// ensureLog returns the record for a job log, ALLOCATING it (kind="log") when
// it does not exist yet. Allocation adopts a sibling pod's record for the same
// coordinate (stable uid), so racing creators converge on one blob. The
// returned record's uid is what the caller appends by. Contention fallback:
// if the allocation LOSES a race (Another pod confirmed the coordinate
// between our lookup and insert), re-read and use the winner.
func (r *fileResolver) ensureLog(ctx context.Context, namespace, name string) (*dbpb.StorageObject, error) {
	rec, err := r.getFile(ctx, namespace, name)
	if err == nil {
		return rec, nil
	}
	if st, ok := status.FromError(err); !(ok && st.Code() == codes.NotFound) {
		return nil, err
	}
	if r.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	alloc, err := r.db.AllocateStorageObject(ctx, &dbpb.AllocateStorageObjectRequest{
		Kind:      "log",
		Namespace: namespace,
		Name:      name,
	})
	if err == nil {
		return alloc, nil
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
		// Lost the race: the winner's record is now visible — re-read it.
		return r.getFile(ctx, namespace, name)
	}
	return nil, err
}

// allocate provisions a record for an upcoming upload (advisory descriptive
// fields; the confirm step settles the observations). An AlreadyExists error
// (coordinate occupied by a VISIBLE record) propagates to the caller.
func (r *fileResolver) allocate(ctx context.Context, req *dbpb.AllocateStorageObjectRequest) (*dbpb.StorageObject, error) {
	if r.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	return r.db.AllocateStorageObject(ctx, req)
}

// confirm settles an allocated record with the OBSERVED size and hash.
func (r *fileResolver) confirm(ctx context.Context, uid string, size int64, sha string) (*dbpb.StorageObject, error) {
	if r.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	return r.db.ConfirmStorageObject(ctx, &dbpb.ConfirmStorageObjectRequest{
		Uid:           uid,
		Size:          size,
		ContentSha256: sha,
	})
}

// syncLogSize reconciles a log record's size with the blob's current size
// (best-effort: a failure is swallowed — the NEXT append retries, and the
// size only feeds UI polling, never correctness).
func (r *fileResolver) syncLogSize(ctx context.Context, uid string, size int64) {
	if r.db == nil {
		return
	}
	_, _ = r.db.SyncLogSize(ctx, &dbpb.SyncLogSizeRequest{Uid: uid, Size: size})
}

// touch records a retrieval (best-effort analytics; errors are swallowed — a
// download is never failed by a failed touch).
func (r *fileResolver) touch(ctx context.Context, uid string) {
	if r.db == nil {
		return
	}
	_, _ = r.db.TouchStorageObject(ctx, &dbpb.TouchStorageObjectRequest{Uid: uid})
}

// delete removes a file: the record first (freed coordinate), then the blob
// (belt and braces — an orphaned blob is inert). A missing file is NotFound.
func (r *fileResolver) delete(ctx context.Context, namespace, name string) error {
	rec, err := r.getFile(ctx, namespace, name)
	if err != nil {
		return err
	}
	if r.db != nil {
		if _, derr := r.db.DeleteStorageObject(ctx, &dbpb.DeleteStorageObjectRequest{Uid: rec.GetUid()}); derr != nil {
			return derr
		}
	}
	if r.blobs != nil {
		_, _ = r.blobs.DeleteBlob(ctx, &artifactspb.BlobRef{Id: rec.GetUid()})
	}
	return nil
}

// listFiles returns the VISIBLE records of a namespace (all namespaces when
// namespace is ""), optionally filtered by kind ("artifact"/"log"; "" = all).
func (r *fileResolver) listFiles(ctx context.Context, namespace, kind string) ([]*dbpb.StorageObject, error) {
	if r.db == nil {
		return nil, status.Error(codes.Unavailable, "database service is not configured")
	}
	resp, err := r.db.ListStorageObjects(ctx, &dbpb.ListStorageObjectsRequest{
		Namespace: namespace,
		Kind:      kind,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetObjects(), nil
}

// contentTypeByName guesses a MIME content type from a file's display name
// (the standard library's extension table; empty when unrecognized). Used by
// the HTTP surfaces that render record-derived file views.
func contentTypeByName(name string) string {
	return mime.TypeByExtension(filepath.Ext(name))
}
