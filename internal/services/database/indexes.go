// Package database — supplemental schema objects (F-30.4 slice 3).
//
// GORM's AutoMigrate expresses indexes through struct tags, which cannot
// express a PARTIAL (filtered) index. The load-bearing invariant of the
// agent/worker topology (F-30.4) needs exactly that: at most ONE ACTIVE
// (status = running) execution per (job_id, agent_name, attempt) coordinate.
// Terminal rows are HISTORY (a rerun RESETS the attempt to 1 and starts a
// fresh execution against the same coordinate — the old terminal row must
// not collide), so the index covers active rows only.
package database

import (
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

// jobExecutionActiveIndexName is the unique index enforcing the
// serial-per-coordinate invariant (below). Deliberately the SAME name on
// both backends so dumps/exhibits read uniformly.
const jobExecutionActiveIndexName = "uk_job_execution_active_coordinate"

// EnsureIndexes creates the schema objects AutoMigrate cannot express.
// Idempotent (IF NOT EXISTS on both backends). Called from Migrate after
// AutoMigrate so the tables/columns it relies on exist.
//
// The job_executions active-coordinate index (F-30.4 slice 3):
//
//	postgresql: a true partial unique index
//	    CREATE UNIQUE INDEX ... ON job_executions (job_id, agent_name, attempt)
//	    WHERE status = 'running'
//	sqlite:     SQLite has no partial indexes, so the FILTER is encoded in an
//	    EXPRESSION COLUMN: the leading term is 0 for active rows and the row's
//	    own id otherwise — two active rows with the same coordinate collide
//	    (tuple (0, job, agent, attempt)), while any mix involving a terminal
//	    row differs in the leading term (term = id ≥ 1, unique per row) and
//	    never collides. Semantically identical to the partial index.
//
// Enforcement: a violating INSERT raises a constraint error; the
// StartJobExecution handler translates it into a routine decline
// (Started:false) — the same shape as the duplicate-episode guard's decline,
// which is the COMMON path (the guard counts first; the index is the
// DEFENSE-IN-DEPTH edge for two racers that both count zero).
func EnsureIndexes(db *gorm.DB, logger *slog.Logger) error {
	stmt, ok := activeCoordinateStatement(db)
	if !ok {
		// Unknown dialect: do not fail the boot over a defense-in-depth
		// object (the episode guard still holds); log loudly.
		logger.Warn("database: unrecognized dialect; skipping supplemental indexes", "dialect", db.Dialector.Name())
		return nil
	}
	if err := db.Exec(stmt).Error; err != nil {
		return fmt.Errorf("database: ensure indexes: %w", err)
	}
	logger.Info("database: supplemental indexes ensured", "index", jobExecutionActiveIndexName)
	return nil
}

// activeCoordinateStatement renders the dialect's unique-active-coordinate
// index statement.
func activeCoordinateStatement(db *gorm.DB) (stmt string, ok bool) {
	switch db.Dialector.Name() {
	case "postgres":
		return fmt.Sprintf(
			"CREATE UNIQUE INDEX IF NOT EXISTS %s ON job_executions (job_id, agent_name, attempt) WHERE status = 'running'",
			jobExecutionActiveIndexName), true
	case "sqlite":
		return fmt.Sprintf(
			"CREATE UNIQUE INDEX IF NOT EXISTS %s ON job_executions (CASE WHEN status = 'running' THEN 0 ELSE id END, job_id, agent_name, attempt)",
			jobExecutionActiveIndexName), true
	default:
		return "", false
	}
}
