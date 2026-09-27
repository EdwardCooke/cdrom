package database_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"cdrom/internal/models"
	"cdrom/internal/services/database"
)

// TestOpenMigrateClose exercises the full backend lifecycle against a
// temporary SQLite file: open, auto-migrate, write a row, read it back,
// close.
func TestOpenMigrateClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg := database.Config{
		Backend:    database.BackendSQLite,
		SQLitePath: dbPath,
	}
	db, err := database.Open(cfg, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close(db)

	if err := database.Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Write a pipeline with a job.
	pipeline := models.Pipeline{Name: "build"}
	if err := db.Create(&pipeline).Error; err != nil {
		t.Fatalf("Create pipeline: %v", err)
	}
	job := models.Job{
		PipelineID:  &pipeline.ID,
		Name:        "compile",
		Status:      models.JobStatusPending,
		TargetGroup: "linux-pool",
	}
	if err := db.Create(&job).Error; err != nil {
		t.Fatalf("Create job: %v", err)
	}

	// Read the pipeline back with its job association.
	var got models.Pipeline
	if err := db.Preload("Jobs").First(&got, pipeline.ID).Error; err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Name != "build" {
		t.Errorf("pipeline name = %q, want %q", got.Name, "build")
	}
	if len(got.Jobs) != 1 || got.Jobs[0].Name != "compile" {
		t.Errorf("jobs = %+v, want one job named %q", got.Jobs, "compile")
	}
}

// TestOpenValidatesConfig ensures each backend requires its connection setting.
func TestOpenValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  database.Config
	}{
		{"sqlite without path", database.Config{Backend: database.BackendSQLite}},
		{"postgres without dsn", database.Config{Backend: database.BackendPostgres}},
		{"unknown backend", database.Config{Backend: "mysql"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := database.Open(tc.cfg, nil); err == nil {
				t.Error("Open: expected error, got nil")
			}
		})
	}
}
