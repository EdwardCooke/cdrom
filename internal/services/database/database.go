package database

import (
	"fmt"
	"log/slog"
	"time"

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
	// TIMESTAMP STANDARD (2026-11-05): every time value written to the
	// database is UTC. Explicit writers use time.Now().UTC() / ts.AsTime()
	// (both UTC); GORM's AUTOTIMESTAMP callbacks (createdAt/updatedAt/
	// deletedAt) honor Config.NowFunc, which DEFAULTS TO time.Now().LOCAL()
	// — the historic source of mixed-zone encodings in the sqlite file. Pin
	// it to UTC so the storage encoding is uniform regardless of the
	// process timezone. Why it matters: the pure-Go sqlite driver
	// (modernc.org/sqlite via glebarez) compares DATETIME operands
	// LEXICALLY, and mixing encoded zones ("2026-11-05 04:00:00Z" vs
	// "2026-11-04 21:00:00-07:00") yields WRONG ORDERINGS for past bounds —
	// the flipped audit/prune predicates behind the five "pre-existing"
	// test failures. Postgres is unaffected (canonical timestamptz), but
	// the standard is backend-neutral by construction. Companion rule:
	// SQL time-range BOUNDS must be bound in the STORAGE ZONE (UTC) — see
	// the .UTC()-annotated Where clauses in server.go.
	gormCfg := gormConfig(logger)
	gormCfg.NowFunc = func() time.Time { return time.Now().UTC() }
	switch cfg.Backend {
	case BackendSQLite:
		db, err = gorm.Open(gormsqlite.Open(cfg.SQLitePath), gormCfg)
	case BackendPostgres:
		db, err = gorm.Open(postgres.Open(cfg.PostgresDSN), gormCfg)
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
