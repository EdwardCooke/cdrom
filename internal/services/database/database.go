package database

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	gormsqlite "github.com/glebarez/sqlite" // pure-Go driver: no cgo, builds on Windows and Linux
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"cdrom/internal/models"
)

// Backend selects the storage engine.
type Backend string

const (
	BackendSQLite   Backend = "sqlite"
	BackendPostgres Backend = "postgres"
)

// ANTI-BUSY MEASURES (SQLite only; established 2026-11-14 after the
// e2e SQLITE_BUSY flake family — a singular writer blowing past the
// driver's default-zero busy tolerance at a peak-overlap):
//
//   - sqliteBusyTimeoutMillis — baked into the DSN so a writer meeting the
//     database-wide lock WAITS briefly instead of failing instantly;
//   - sqliteMaxOpenConns — caps the pool, because SQLite SERIALIZES all
//     writers on one lock: beyond a handful of queued writers you buy only
//     SQLITE_BUSY odds, not speed. Reads (the mesh's constant polling)
//     tolerate the queue.
const (
	sqliteBusyTimeoutMillis = 5000
	sqliteMaxOpenConns      = 8
)

// Config describes how to reach the storage backend.
type Config struct {
	Backend Backend
	// SQLitePath is the file path for the SQLite database (used when
	// Backend == BackendSQLite). ":memory:" is supported for tests.
	// SQLitePath must be set for the sqlite backend.
	//
	// DSN CONTRACT: the value may carry the driver's own options
	// (query-string form, e.g. "db.sqlite?_pragma=busy_timeout=10000&_pragma=
	// journal_mode=wal"). When it does, it is honored VERBATIM — the caller
	// owns the posture (the e2e harness relies on this to arm WAL + a 10 s
	// busy tolerance). Otherwise Open() appends a finite busy_timeout
	// backstop itself (ANTI-BUSY measures below).
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
		// Finite busy tolerance: a writer meeting the database-wide lock
		// WAITS up to sqliteBusyTimeoutMillis instead of failing instantly.
		// Armed ONLY for pristine paths — a path that already speaks the
		// driver's option grammar is caller-owned and respected verbatim
		// (concatenating onto it corrupts the DSN; the e2e harness arms
		// its own, stronger posture this way).
		dsn := cfg.SQLitePath
		if !strings.Contains(dsn, "?") {
			dsn += "?_pragma=busy_timeout=" + strconv.Itoa(sqliteBusyTimeoutMillis)
		}
		db, err = gorm.Open(gormsqlite.Open(dsn), gormCfg)
	case BackendPostgres:
		db, err = gorm.Open(postgres.Open(cfg.PostgresDSN), gormCfg)
	}
	if err != nil {
		return nil, fmt.Errorf("database: open: %w", err)
	}
	if cfg.Backend == BackendSQLite {
		// Cap the pool: SQLite serializes writers on one lock, so extra
		// conns buy only contention (see the ANTI-BUSY note above).
		sqlDB, err := db.DB()
		if err != nil {
			return nil, fmt.Errorf("database: open: %w", err)
		}
		sqlDB.SetMaxOpenConns(sqliteMaxOpenConns)
	}
	logger.Info("database: connected", "backend", cfg.Backend)
	return db, nil
}

// Migrate applies the schema for all registered models (GORM AutoMigrate:
// creates missing tables and adds missing columns automatically), then the
// SUPPLEMENTAL OBJECTS AutoMigrate cannot express (indexes.go — notably the
// job_executions active-coordinate unique index, F-30.4 slice 3).
func Migrate(db *gorm.DB, logger *slog.Logger) error {
	if err := db.AutoMigrate(models.All()...); err != nil {
		return fmt.Errorf("database: migrate: %w", err)
	}
	if err := EnsureIndexes(db, logger); err != nil {
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
		loggerConfig := gormlogger.Config{IgnoreRecordNotFoundError: true}

		cfg.Logger = gormlogger.NewSlogLogger(logger, loggerConfig)
	}
	return cfg
}
