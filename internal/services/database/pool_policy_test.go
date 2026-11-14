package database_test

import (
	"bytes"
	"cdrom/internal/services/database"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// TestSQLitePoolCapsAndBusyTimeout pins the anti-BUSY measures wired into
// Open() (database.go): the pool ceiling and the FINITE busy_timeout baked
// into the DSN. Without them a writer meeting the database-wide lock at a
// peak overlap fails instantly (the e2e SQLITE_BUSY flake family).
func TestSQLitePoolCapsAndBusyTimeout(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(new(bytes.Buffer), nil))

	db, err := database.Open(database.Config{
		Backend:    database.BackendSQLite,
		SQLitePath: filepath.Join(t.TempDir(), "caps.db"),
	}, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close(db)

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB: %v", err)
	}
	const wantMaxOpen = 8 // mirrors sqliteMaxOpenConns in database.go
	// The cap is READ through the pool stats (sql.DB exposes no symmetric
	// getter in this toolchain; Stats().MaxOpenConnections is the
	// documented face of the SetMaxOpenConns knob).
	if got := sqlDB.Stats().MaxOpenConnections; got != wantMaxOpen {
		t.Errorf("Stats().MaxOpenConnections = %d, want %d", got, wantMaxOpen)
	}

	// The driver exposes pragmas through a function-like SQL surface:
	// pragma_busy_timeout() yields a single `timeout` column with the
	// EFFECTIVE value (introspected live: one row, col[0]). Demand finiteness
	// — the default is 0, so >0 proves the DSN measure took effect.
	var bt int64
	if err := db.Raw("SELECT * FROM pragma_busy_timeout()").Scan(&bt).Error; err != nil {
		t.Skipf("driver lacks the pragma_busy_timeout() surface (%v); the DSN "+
			"opened, so the measure is wired but unreadable here", err)
	}
	if bt <= 0 {
		t.Errorf("effective busy_timeout = %d ms, want > 0 (finite tolerance)", bt)
	}
}

// TestCallerOwnedDSNHonoredVerbatim pins the DSN CONTRACT (database.go):
// a path that ALREADY speaks the driver's option grammar is respected
// verbatim — the caller owns the posture (the e2e harness arms WAL + a 10 s
// busy tolerance this way) — and naive concatenation must NOT corrupt it.
func TestCallerOwnedDSNHonoredVerbatim(t *testing.T) {
	t.Parallel()

	buf := new(bytes.Buffer)
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	// The harness's exact shape: ?_pragma=busy_timeout=<ms>_pragma=form.
	harnessLike := filepath.Join(t.TempDir(), "harness.db") +
		"?_pragma=busy_timeout=10000&_pragma=journal_mode=wal"
	db, err := database.Open(database.Config{
		Backend:    database.BackendSQLite,
		SQLitePath: harnessLike,
	}, logger)
	if err != nil {
		t.Fatalf("Open(harness-like DSN): %v", err)
	}
	defer database.Close(db)
	if err := database.Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Effective busy tolerance must be the CALLER'S 10000, not the
	// backstop's 5000 — proof the verbatim path won.
	var bt int64
	if err := db.Raw("SELECT * FROM pragma_busy_timeout()").Scan(&bt).Error; err != nil {
		t.Skipf("driver lacks the pragma_busy_timeout() surface (%v)", err)
	}
	if bt != 10000 {
		t.Errorf("effective busy_timeout = %d ms, want 10000 (caller-owned)", bt)
	}

	// WAL really engaged (the harness's other pragma).
	var jm string
	if err := db.Raw("SELECT * FROM pragma_journal_mode()").Scan(&jm).Error; err != nil {
		t.Skipf("driver lacks the pragma_journal_mode() surface (%v)", err)
	}
	if !strings.EqualFold(jm, "wal") {
		t.Errorf("journal_mode = %q, want wal", jm)
	}
}
