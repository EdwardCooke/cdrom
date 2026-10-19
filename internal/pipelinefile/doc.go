// Package pipelinefile is the compiler for Cdrom pipeline definition files
// (F-21.1). A definition is a single YAML document — a Kubernetes-style
// manifest with an apiVersion, a kind, a metadata block, and a spec — and this
// package parses it (preserving key order), validates the manifest envelope,
// and dispatches on kind. It is the single place a human-authored, name-based
// file is reconciled with the database schema.
//
// This package is the skeleton the rest of F-21.1 builds on (F-21.1.1): it
// owns the manifest envelope (apiVersion / kind / metadata / spec), the kind
// dispatch, the key-order-preserving parse, and the actionable validation
// errors. The sub-features F-21.1.2 (steps), F-21.1.3 (variables and secret
// sources), and F-21.1.4 (jobs and the import/export API) interpret a
// document's spec and map it onto the schema.
//
// The file is deliberately not the database schema. The compiler is the
// bridge: it validates the definition and produces a typed, in-memory
// representation (a Document with an ordered Spec) that the sub-features fill
// in. The file's names never reach an execution target.
package pipelinefile
