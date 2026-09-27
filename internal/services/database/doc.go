// Package database abstracts the storage backend. All persistence goes
// through this package: SQLite for local development, PostgreSQL for
// deployments. Code outside this package must not import driver-specific
// libraries directly.
package database
