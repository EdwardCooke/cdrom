// Package artifacts implements the cdrom.artifacts.v1.Artifacts gRPC
// service: a general-purpose BLOB STORE. It stores and retrieves raw bytes
// identified exclusively by an opaque id (a UUID assigned by the caller); it
// knows NOTHING about names, namespaces, packages, or any other attribute of
// the bytes it holds.
//
// All FILE METADATA lives in the Database service's storage_object records
// (F-21.6 storage redesign): what a file IS (kind, namespace, display name,
// size, content hash, package coordinates, counters) is a database fact;
// WHAT IT CONTAINS is a blob here. Callers translate (namespace, name) <->
// uid at the API/service boundary and speak uids to this service.
//
// The gRPC Server (server.go) handles the streaming/chunking protocol and
// delegates the actual I/O to a Store (store.go). The default store is the
// local filesystem (blobfs.go); the store kind is configurable
// (artifacts_store / CDROM_ARTIFACTS_STORE) so other built-in backends (e.g.
// S3, Azure Blob) can be added without changing the gRPC surface.
//
// Blobs are sharded flat under a root directory:
//
//	<root>/blobs/<two hex chars of the id>/<full id>
//
// so a million blobs spread over ~1,300 directories stay enumerable. The id
// is validated strictly ([0-9a-zA-Z-]{1,64}) — typically a UUID — which
// makes the on-disk placement safe without any escaping.
package artifacts
