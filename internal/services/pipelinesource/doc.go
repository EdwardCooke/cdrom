// Package pipelinesource implements the cdrom.pipelinesource.v1.PipelineSource
// gRPC service (F-21.2): the source-fetcher. It performs the actual fetch of a
// pipeline definition file named by a *reference* (a "<type>:<spec>" string —
// git / http / file) and returns the file's content, its version identifier,
// and its content hash.
//
// The fetcher is a standalone process (cmd/pipelinesource) dialed only by the
// API over gRPC (mutual TLS when a CA is configured). It holds no pipeline
// definitions, no credentials, and no encryption key at rest; its only on-disk
// state is a transient git repository cache (F-21.2.3).
//
// The pieces:
//   - reference.go (F-21.2.2) parses a reference into a kind + locator.
//   - cache.go (F-21.2.3) is the on-disk git repository cache: the cache root,
//     the atomic text index, the per-repository lock, and the cleanup thread.
//   - git.go / http.go / file.go (F-21.2.4) are the three fetch clients.
//   - server.go (F-21.2.1) is the gRPC server that dispatches FetchSource to
//     the right client.
package pipelinesource
