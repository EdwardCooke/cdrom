// Package artifacts implements the cdrom.artifacts.v1.Artifacts gRPC service:
// a general-purpose, namespaced file store for artifacts (immutable files)
// and logs (append-only files).
//
// Every file lives in a *namespace*: an opaque scope string that groups
// related files. The service is agnostic about what a namespace means — it is
// up to the caller. A job's artifacts and logs use the job's id as the
// namespace, while a deployed release's artifacts might use a release
// identifier. This keeps the store general enough to hold both job-owned
// artifacts and deployed/published artifacts.
//
// The gRPC Server (server.go) handles the streaming/chunking protocol and
// delegates the actual I/O to a Store (store.go). The default store is the
// local filesystem (filesystem.go); the store kind is configurable
// (artifacts_store / CDROM_ARTIFACTS_STORE) so other built-in backends (e.g.
// S3, Azure Blob) can be added without changing the gRPC surface.
//
// Files are stored under a root directory, one subdirectory per namespace:
// <root>/<namespace>/<name>. Logs are stored under <root>/<namespace>/logs/
// and are appended to as they grow, so a log can be read while it is still
// being written (tail) and replayed in full afterwards.
package artifacts
