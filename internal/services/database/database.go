package database

import (
	"fmt"
	"log/slog"

	gormsqlite "github.com/glebarez/sqlite" // pure-Go driver: no cgo, builds on Windows and Linux
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"cdrom/internal/models"
)

// Backend selects the storage engine.
type Backend string

const (
	BackendSQLite   Backend = "sqlite"
	BackendPostgres Backend = "postgres"
)

// Config describes how to reach the storage backend.
type Config struct {
	Backend Backend
	// SQLitePath is the file path for the SQLite database (used when
	// Backend == BackendSQLite). ":memory:" is supported for tests.
	// SQLitePath must be set for the sqlite backend.
	SQLitePath string
	// PostgresDSN is the connection string for PostgreSQL (used when
	// Backend == BackendPostgres), e.g.
	// "host=... user=... password=... dbname=... sslmode=...".
	// PostgresDSN must be set for the postgres backend.
	PostgresDSN string
}

// Validate checks that the configuration is complete for the selected backend.
func (c Config) Validate() error {
	switch c.Backend {
	case BackendSQLite:
		if c.SQLitePath == "" {
			return fmt.Errorf("database: sqlite backend requires SQLitePath")
		}
	case BackendPostgres:
		if c.PostgresDSN == "" {
			return fmt.Errorf("database: postgres backend requires PostgresDSN")
		}
	default:
		return fmt.Errorf("database: unknown backend %q", c.Backend)
	}
	return nil
}

// Open connects to the configured storage backend.
func Open(cfg Config, logger *slog.Logger) (*gorm.DB, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var (
		db  *gorm.DB
		err error
	)
	switch cfg.Backend {
	case BackendSQLite:
		db, err = gorm.Open(gormsqlite.Open(cfg.SQLitePath), gormConfig(logger))
	case BackendPostgres:
		db, err = gorm.Open(postgres.Open(cfg.PostgresDSN), gormConfig(logger))
	}
	if err != nil {
		return nil, fmt.Errorf("database: open: %w", err)
	}
	logger.Info("database: connected", "backend", cfg.Backend)
	return db, nil
}

// Migrate applies the schema for all registered models (GORM AutoMigrate:
// creates missing tables and adds missing columns automatically).
func Migrate(db *gorm.DB, logger *slog.Logger) error {
	if err := db.AutoMigrate(models.All()...); err != nil {
		return fmt.Errorf("database: migrate: %w", err)
	}
	logger.Info("database: schema migrated")
	return nil
}

// Close closes the underlying database connection pool.
func Close(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("database: close: %w", err)
	}
	return sqlDB.Close()
}

func gormConfig(logger *slog.Logger) *gorm.Config {
	cfg := &gorm.Config{}
	if logger != nil {
		cfg.Logger = gormLogger{logger: logger}
	}
	return cfg
}
