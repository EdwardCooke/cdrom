// Package worker implements the ephemeral worker: a short-lived process
// spawned in Kubernetes to execute a one-off job, then terminate. Must run
// on both Windows and Linux.
package worker
