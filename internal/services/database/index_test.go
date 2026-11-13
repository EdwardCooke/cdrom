// Package database — the job_executions ACTIVE-COORDINATE unique index
// (F-30.4 slice 3): schema-level proof that two ACTIVE executions may not
// share a (job_id, agent_name, attempt) coordinate, while TERMINAL history
// rows never collide (a rerun resets the attempt and starts fresh against
// the same coordinate).
package database

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cdrom/internal/models"
	"gorm.io/gorm"
)

// openIndexedTestDB opens a disposable SQLite database with the full schema
// (AutoMigrate + the supplemental indexes — Migrate runs both).
func openIndexedTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "indexed.db")
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	db, err := Open(Config{Backend: BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = Close(db) })
	if err := Migrate(db, logger); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// TestActiveCoordinateIndexRejectsTwin proves the index's SEMANTICS on the
// real dialect: a second ACTIVE row for the same coordinate is REFUSED
// (recognized as OUR violation, not some other unique constraint), while a
// TERMINAL row for the same coordinate is ACCEPTED (history is lawful).
func TestActiveCoordinateIndexRejectsTwin(t *testing.T) {
	db := openIndexedTestDB(t)
	now := time.Now().UTC()
	mk := func(status models.JobStatus) *models.JobExecution {
		return &models.JobExecution{
			JobID:     1,
			AgentName: "agent-x",
			Attempt:   1,
			Status:    status,
			StartedAt: &now,
		}
	}
	if err := db.Create(mk(models.JobStatusRunning)).Error; err != nil {
		t.Fatalf("seed active execution: %v", err)
	}
	// Twin ACTIVE row: REFUSED, and the error is recognizable as ours.
	err := db.Create(mk(models.JobStatusRunning)).Error
	if err == nil {
		t.Fatal("twin active execution was INSERTED (want the unique index to refuse it)")
	}
	if !isActiveCoordinateViolation(err) {
		t.Fatalf("violation not recognized as the active-coordinate index: %v", err)
	}
	// Retire the first row (terminal): the coordinate is now FREE AGAIN.
	if err := db.Model(&models.JobExecution{}).
		Where("job_id = ? AND agent_name = ? AND attempt = ? AND status = ?",
			uint(1), "agent-x", 1, models.JobStatusRunning).
		UpdateColumn("status", models.JobStatusSucceeded).Error; err != nil {
		t.Fatalf("retire first execution: %v", err)
	}
	// A TERMINAL history row for the same coordinate: LAWFUL (rows of
	// differing terminal statuses coexist — history is not exclusive).
	if err := db.Create(mk(models.JobStatusFailed)).Error; err != nil {
		t.Fatalf("terminal history row refused: %v", err)
	}
	// A fresh ACTIVE row (the rerun's generation): LAWFUL — the terminal
	// rows' expression terms are their own ids, not 0.
	if err := db.Create(mk(models.JobStatusRunning)).Error; err != nil {
		t.Fatalf("second-generation active row refused: %v", err)
	}
}
