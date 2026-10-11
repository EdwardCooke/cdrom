package database

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// testLimitRule is one concurrency limit rule a test seeds into the database
// (F-16): a kind (global / group / key), a regex pattern, and a limit.
type testLimitRule struct {
	kind    string
	pattern string
	limit   int
}

// startConcurrencyServer opens an in-memory SQLite database, migrates it,
// seeds the given concurrency limit rules (F-16) into the database, and serves
// the Database gRPC service over an in-process bufconn listener. It returns
// the gRPC client and the underlying GORM handle (the latter lets a test set
// up a state the RPCs would not, such as a stuck job for the reconcile test).
func startConcurrencyServer(t *testing.T, rules []testLimitRule) (dbpb.DatabaseClient, *gorm.DB) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(Config{Backend: BackendSQLite, SQLitePath: dbPath}, logger)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = Close(db) })
	if err := Migrate(db, logger); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Seed the concurrency limit rules (F-16) directly into the database, so
	// the server resolves them from the database (not configuration).
	for _, r := range rules {
		if err := db.Create(&models.ConcurrencyLimit{Kind: r.kind, Pattern: r.pattern, Limit: r.limit}).Error; err != nil {
			t.Fatalf("seed limit rule: %v", err)
		}
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	dbpb.RegisterDatabaseServer(srv, NewServer(db))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return dbpb.NewDatabaseClient(conn), db
}

// createJobWithKeys creates a standalone job (no pipeline) whose spec carries
// the given concurrency keys and target group, and returns its id.
func createJobWithKeys(t *testing.T, client dbpb.DatabaseClient, name, group string, keys []string) int64 {
	t.Helper()
	job, err := client.CreateJob(context.Background(), &dbpb.CreateJobRequest{
		Name:        name,
		TargetGroup: group,
		Spec:        &dbpb.JobSpec{ConcurrencyKeys: keys},
	})
	if err != nil {
		t.Fatalf("CreateJob(%s): %v", name, err)
	}
	return job.GetId()
}

// TestAcquireJobSlotsKeyLock verifies that a concurrency key is a lock (F-16):
// the first job holding the key is admitted, and a second job sharing the key
// is blocked (reported with the key in blocked_by) until the first releases.
func TestAcquireJobSlotsKeyLock(t *testing.T) {
	client, _ := startConcurrencyServer(t, nil)
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "", []string{"prod-db"})
	b := createJobWithKeys(t, client, "b", "", []string{"prod-db"})

	// The first job acquires the key.
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a})
	if err != nil {
		t.Fatalf("AcquireJobSlots(a): %v", err)
	}
	if !resp.GetAcquired() {
		t.Fatalf("job a acquired = false, want true (the key is free)")
	}

	// The second job, sharing the key, is blocked.
	resp, err = client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b})
	if err != nil {
		t.Fatalf("AcquireJobSlots(b): %v", err)
	}
	if resp.GetAcquired() {
		t.Fatalf("job b acquired = true, want false (the key is held by a)")
	}
	if len(resp.GetBlockedBy()) != 1 || resp.GetBlockedBy()[0] != "prod-db" {
		t.Errorf("job b blocked_by = %v, want [prod-db]", resp.GetBlockedBy())
	}

	// Releasing a frees the key, so b can now acquire it.
	if _, err := client.ReleaseJobSlots(ctx, &dbpb.ReleaseJobSlotsRequest{JobId: a}); err != nil {
		t.Fatalf("ReleaseJobSlots(a): %v", err)
	}
	resp, err = client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b})
	if err != nil {
		t.Fatalf("AcquireJobSlots(b) after release: %v", err)
	}
	if !resp.GetAcquired() {
		t.Errorf("job b acquired = false after a released, want true")
	}
}

// TestAcquireJobSlotsGroupLimit verifies that a per-group limit bounds how
// many jobs in the group run at once (F-16): with a limit of 1, the second
// job in the group is blocked on the group resource.
func TestAcquireJobSlotsGroupLimit(t *testing.T) {
	client, _ := startConcurrencyServer(t, []testLimitRule{{kind: "group", pattern: "pool-a", limit: 1}})
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "pool-a", nil)
	b := createJobWithKeys(t, client, "b", "pool-a", nil)

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b})
	if err != nil {
		t.Fatalf("AcquireJobSlots(b): %v", err)
	}
	if resp.GetAcquired() {
		t.Fatalf("job b acquired = true, want false (group pool-a is at its limit)")
	}
	if len(resp.GetBlockedBy()) != 1 || resp.GetBlockedBy()[0] != "group:pool-a" {
		t.Errorf("job b blocked_by = %v, want [group:pool-a]", resp.GetBlockedBy())
	}
}

// TestAcquireJobSlotsGlobalLimit verifies that a global limit bounds the total
// number of running jobs across all groups (F-16).
func TestAcquireJobSlotsGlobalLimit(t *testing.T) {
	client, _ := startConcurrencyServer(t, []testLimitRule{{kind: "global", pattern: "", limit: 1}})
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "pool-a", nil)
	b := createJobWithKeys(t, client, "b", "pool-b", nil)

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b})
	if err != nil {
		t.Fatalf("AcquireJobSlots(b): %v", err)
	}
	if resp.GetAcquired() {
		t.Fatalf("job b acquired = true, want false (the global limit is reached)")
	}
	if len(resp.GetBlockedBy()) != 1 || resp.GetBlockedBy()[0] != "global" {
		t.Errorf("job b blocked_by = %v, want [global]", resp.GetBlockedBy())
	}
}

// TestAcquireJobSlotsMultipleKeysOR verifies that a job's keys are OR'd (F-16):
// the job is blocked if *any* of its keys is held, and it holds *all* of them
// while it runs.
func TestAcquireJobSlotsMultipleKeysOR(t *testing.T) {
	client, _ := startConcurrencyServer(t, nil)
	ctx := context.Background()

	// A job that holds "k1" (and nothing else).
	other := createJobWithKeys(t, client, "other", "", []string{"k1"})
	// A job that holds both "k1" and "k2".
	both := createJobWithKeys(t, client, "both", "", []string{"k1", "k2"})

	// Acquire "other" (holds k1).
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: other}); err != nil || !resp.GetAcquired() {
		t.Fatalf("other: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}

	// "both" is blocked because k1 is held (even though k2 is free).
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: both})
	if err != nil {
		t.Fatalf("AcquireJobSlots(both): %v", err)
	}
	if resp.GetAcquired() {
		t.Fatalf("both acquired = true, want false (k1 is held)")
	}
	if len(resp.GetBlockedBy()) != 1 || resp.GetBlockedBy()[0] != "k1" {
		t.Errorf("both blocked_by = %v, want [k1] (only k1 is held)", resp.GetBlockedBy())
	}

	// Release "other"; now both k1 and k2 are free, so "both" acquires and
	// holds both keys.
	if _, err := client.ReleaseJobSlots(ctx, &dbpb.ReleaseJobSlotsRequest{JobId: other}); err != nil {
		t.Fatalf("ReleaseJobSlots(other): %v", err)
	}
	resp, err = client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: both})
	if err != nil {
		t.Fatalf("AcquireJobSlots(both) after release: %v", err)
	}
	if !resp.GetAcquired() {
		t.Fatalf("both acquired = false after release, want true")
	}

	// A job sharing either key is now blocked (both keys are held).
	third := createJobWithKeys(t, client, "third", "", []string{"k2"})
	resp, err = client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: third})
	if err != nil {
		t.Fatalf("AcquireJobSlots(third): %v", err)
	}
	if resp.GetAcquired() {
		t.Fatalf("third acquired = true, want false (k2 is held by both)")
	}
}

// TestAcquireJobSlotsIdempotent verifies that admitting an already-admitted
// job is a no-op (it does not double-acquire its slots, F-16).
func TestAcquireJobSlotsIdempotent(t *testing.T) {
	client, _ := startConcurrencyServer(t, nil)
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "", []string{"k"})

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("first acquire: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	// A second acquire is a no-op (already admitted); it must not error.
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if resp.GetAcquired() {
		t.Errorf("second acquire reported acquired=true, want a no-op (already admitted)")
	}

	// A job sharing the key is still blocked (the slot was not double-held).
	b := createJobWithKeys(t, client, "b", "", []string{"k"})
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || resp.GetAcquired() {
		t.Fatalf("job b: acquired=%v err=%v, want blocked", resp.GetAcquired(), err)
	}
}

// TestReleaseJobSlotsFreesSlot verifies that releasing a job's slots frees the
// resources it held (F-16): after a release, a job sharing the resources can
// be admitted.
func TestReleaseJobSlotsFreesSlot(t *testing.T) {
	client, _ := startConcurrencyServer(t, []testLimitRule{{kind: "group", pattern: "pool-a", limit: 1}})
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "pool-a", []string{"k"})
	b := createJobWithKeys(t, client, "b", "pool-a", []string{"k"})

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	// b is blocked on both the group and the key.
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || resp.GetAcquired() {
		t.Fatalf("job b: acquired=%v err=%v, want blocked", resp.GetAcquired(), err)
	}

	// Releasing a frees both the group slot and the key.
	if _, err := client.ReleaseJobSlots(ctx, &dbpb.ReleaseJobSlotsRequest{JobId: a}); err != nil {
		t.Fatalf("ReleaseJobSlots(a): %v", err)
	}
	resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b})
	if err != nil {
		t.Fatalf("AcquireJobSlots(b) after release: %v", err)
	}
	if !resp.GetAcquired() {
		t.Errorf("job b acquired = false after a released, want true")
	}
}

// TestGetQueueStatus verifies the queue view (F-16): it reports the pending
// jobs (each with the resource(s) blocking it), the per-group load, the global
// load, and the per-key load.
func TestGetQueueStatus(t *testing.T) {
	client, _ := startConcurrencyServer(t, []testLimitRule{
		{kind: "global", pattern: "", limit: 2},
		{kind: "group", pattern: "pool-a", limit: 1},
	})
	ctx := context.Background()

	// A job holding the pool-a group slot and the "k" key.
	a := createJobWithKeys(t, client, "a", "pool-a", []string{"k"})
	// A job blocked on the group (and the key).
	b := createJobWithKeys(t, client, "b", "pool-a", []string{"k"})
	// A job blocked on the key only (different group).
	c := createJobWithKeys(t, client, "c", "pool-b", []string{"k"})

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	// Mark a running so the group/global load counts it.
	if _, err := client.UpdateJob(ctx, &dbpb.UpdateJobRequest{Id: a, Status: dbpb.JobStatus_JOB_STATUS_RUNNING}); err != nil {
		t.Fatalf("UpdateJob(a running): %v", err)
	}

	queue, err := client.GetQueueStatus(ctx, &dbpb.GetQueueStatusRequest{})
	if err != nil {
		t.Fatalf("GetQueueStatus: %v", err)
	}

	// Job a is running (not pending); b and c are the pending jobs.
	if got := len(queue.GetPending()); got != 2 {
		t.Fatalf("pending = %d, want 2 (b and c)", got)
	}
	// The global load counts the one running job.
	if queue.GetGlobalRunning() != 1 {
		t.Errorf("global_running = %d, want 1", queue.GetGlobalRunning())
	}
	if queue.GetGlobalLimit() != 2 {
		t.Errorf("global_limit = %d, want 2", queue.GetGlobalLimit())
	}

	// The per-group load for pool-a: one running, limit 1.
	var poolA *dbpb.QueueGroupLoad
	for _, g := range queue.GetGroups() {
		if g.GetGroup() == "pool-a" {
			poolA = g
		}
	}
	if poolA == nil {
		t.Fatalf("no group load for pool-a")
	}
	if poolA.GetRunning() != 1 || poolA.GetLimit() != 1 {
		t.Errorf("pool-a load = running %d limit %d, want running 1 limit 1", poolA.GetRunning(), poolA.GetLimit())
	}

	// The per-key load for "k": one holder, limit 1.
	var keyK *dbpb.QueueKeyLoad
	for _, k := range queue.GetKeys() {
		if k.GetKey() == "k" {
			keyK = k
		}
	}
	if keyK == nil {
		t.Fatalf("no key load for k")
	}
	if keyK.GetHolders() != 1 || keyK.GetLimit() != 1 {
		t.Errorf("key k load = holders %d limit %d, want holders 1 limit 1", keyK.GetHolders(), keyK.GetLimit())
	}

	// Job b is blocked on both the group and the key; job c on the key only.
	blocked := map[int64][]string{}
	for _, p := range queue.GetPending() {
		blocked[p.GetId()] = p.GetBlockedBy()
	}
	if got := blocked[b]; len(got) != 2 {
		t.Errorf("job b blocked_by = %v, want 2 resources (group + key)", got)
	}
	if got := blocked[c]; len(got) != 1 || got[0] != "k" {
		t.Errorf("job c blocked_by = %v, want [k]", got)
	}
}

// TestReconcileConcurrency verifies that ReconcileConcurrency releases the
// slots of a job that reached a terminal state but still holds them (a missed
// release, F-16), so a stuck job cannot wedge a resource forever.
func TestReconcileConcurrency(t *testing.T) {
	client, db := startConcurrencyServer(t, nil)
	ctx := context.Background()

	a := createJobWithKeys(t, client, "a", "", []string{"k"})
	b := createJobWithKeys(t, client, "b", "", []string{"k"})

	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	// Simulate a missed release: a reaches a terminal state but still holds
	// the key (its admitted flag is not cleared). The normal terminal-status
	// RPCs (UpdateJob, ReapJob, …) release the slots, so to reproduce the
	// crash scenario reconcile exists to catch, set the status directly,
	// bypassing the release hook.
	if err := db.Model(&models.Job{}).Where("id = ?", a).
		Update("status", models.JobStatusSucceeded).Error; err != nil {
		t.Fatalf("set a succeeded directly: %v", err)
	}
	// b is blocked while a still holds the key.
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || resp.GetAcquired() {
		t.Fatalf("job b: acquired=%v err=%v, want blocked", resp.GetAcquired(), err)
	}

	// Reconcile releases a's stuck slots.
	resp, err := client.ReconcileConcurrency(ctx, &dbpb.ReconcileConcurrencyRequest{})
	if err != nil {
		t.Fatalf("ReconcileConcurrency: %v", err)
	}
	if resp.GetReleased() != 1 {
		t.Errorf("reconcile released = %d, want 1", resp.GetReleased())
	}

	// Now b can acquire the freed key.
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job b after reconcile: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
}

// TestConcurrencyLimitCRUD verifies the concurrency limit rule CRUD RPCs
// (F-16): create, list (with and without a kind filter), update, and delete,
// plus validation (an invalid kind, an invalid pattern, a negative limit, and
// a duplicate rule are all rejected).
func TestConcurrencyLimitCRUD(t *testing.T) {
	client, _ := startConcurrencyServer(t, nil)
	ctx := context.Background()

	// Create a key limit rule.
	created, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{
		Kind: "key", Pattern: "^prod-.*", Limit: 2,
	})
	if err != nil {
		t.Fatalf("CreateConcurrencyLimit: %v", err)
	}
	if created.GetId() == 0 {
		t.Fatalf("created rule has no id")
	}

	// Validation: an invalid kind is rejected.
	if _, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{Kind: "bogus", Limit: 1}); err == nil {
		t.Errorf("CreateConcurrencyLimit(bogus kind) = nil error, want an error")
	}
	// Validation: an invalid (non-compiling) pattern is rejected.
	if _, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{Kind: "key", Pattern: "(", Limit: 1}); err == nil {
		t.Errorf("CreateConcurrencyLimit(bad pattern) = nil error, want an error")
	}
	// Validation: a negative limit is rejected.
	if _, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{Kind: "key", Pattern: "x", Limit: -1}); err == nil {
		t.Errorf("CreateConcurrencyLimit(negative limit) = nil error, want an error")
	}
	// Validation: a duplicate (kind, pattern) rule is rejected.
	if _, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{Kind: "key", Pattern: "^prod-.*", Limit: 3}); err == nil {
		t.Errorf("CreateConcurrencyLimit(duplicate) = nil error, want an error")
	}

	// List (all kinds) returns the one rule.
	list, err := client.ListConcurrencyLimits(ctx, &dbpb.ListConcurrencyLimitsRequest{})
	if err != nil {
		t.Fatalf("ListConcurrencyLimits: %v", err)
	}
	if len(list.GetLimits()) != 1 {
		t.Fatalf("list = %d rules, want 1", len(list.GetLimits()))
	}
	// List filtered by kind returns the rule.
	listKey, err := client.ListConcurrencyLimits(ctx, &dbpb.ListConcurrencyLimitsRequest{Kind: "key"})
	if err != nil {
		t.Fatalf("ListConcurrencyLimits(key): %v", err)
	}
	if len(listKey.GetLimits()) != 1 {
		t.Errorf("list(key) = %d rules, want 1", len(listKey.GetLimits()))
	}
	// List filtered by an unrelated kind returns none.
	listGroup, err := client.ListConcurrencyLimits(ctx, &dbpb.ListConcurrencyLimitsRequest{Kind: "group"})
	if err != nil {
		t.Fatalf("ListConcurrencyLimits(group): %v", err)
	}
	if len(listGroup.GetLimits()) != 0 {
		t.Errorf("list(group) = %d rules, want 0", len(listGroup.GetLimits()))
	}

	// Update the rule's limit.
	updated, err := client.UpdateConcurrencyLimit(ctx, &dbpb.UpdateConcurrencyLimitRequest{
		Id:    created.GetId(),
		Limit: int32Ptr(5),
	})
	if err != nil {
		t.Fatalf("UpdateConcurrencyLimit: %v", err)
	}
	if updated.GetLimit() != 5 {
		t.Errorf("updated limit = %d, want 5", updated.GetLimit())
	}

	// Delete the rule.
	if _, err := client.DeleteConcurrencyLimit(ctx, &dbpb.DeleteConcurrencyLimitRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("DeleteConcurrencyLimit: %v", err)
	}
	list, err = client.ListConcurrencyLimits(ctx, &dbpb.ListConcurrencyLimitsRequest{})
	if err != nil {
		t.Fatalf("ListConcurrencyLimits after delete: %v", err)
	}
	if len(list.GetLimits()) != 0 {
		t.Errorf("list after delete = %d rules, want 0", len(list.GetLimits()))
	}
}

// int32Ptr returns a pointer to v (for the optional limit field on an update).
func int32Ptr(v int32) *int32 { return &v }

// TestConcurrencyLimitRegexResolution verifies that a concrete resource's
// effective limit is resolved from the regex limit rules (F-16): the most
// specific (longest) matching pattern wins, and a resource matching no rule
// falls back to the default (a key defaults to a limit of 1 — a lock).
func TestConcurrencyLimitRegexResolution(t *testing.T) {
	client, _ := startConcurrencyServer(t, []testLimitRule{
		// A broad rule matching any "prod-*" key with a limit of 2.
		{kind: "key", pattern: "^prod-.*", limit: 2},
		// A more specific rule matching "prod-db" with a limit of 1; it wins
		// over the broader rule for that key (longest pattern).
		{kind: "key", pattern: "^prod-db$", limit: 1},
	})
	ctx := context.Background()

	// "prod-db" matches both rules; the most specific (limit 1) wins, so it
	// is a lock: the second job holding it is blocked.
	a := createJobWithKeys(t, client, "a", "", []string{"prod-db"})
	b := createJobWithKeys(t, client, "b", "", []string{"prod-db"})
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || resp.GetAcquired() {
		t.Fatalf("job b: acquired=%v err=%v, want blocked (prod-db limit is 1)", resp.GetAcquired(), err)
	}

	// "prod-web" matches only the broad rule (limit 2), so two jobs can hold
	// it.
	c := createJobWithKeys(t, client, "c", "", []string{"prod-web"})
	d := createJobWithKeys(t, client, "d", "", []string{"prod-web"})
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: c}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job c: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: d}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job d: acquired=%v err=%v, want acquired (prod-web limit is 2)", resp.GetAcquired(), err)
	}

	// "other" matches no rule, so it defaults to a limit of 1 (a lock).
	e := createJobWithKeys(t, client, "e", "", []string{"other"})
	f := createJobWithKeys(t, client, "f", "", []string{"other"})
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: e}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job e: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: f}); err != nil || resp.GetAcquired() {
		t.Fatalf("job f: acquired=%v err=%v, want blocked (other defaults to a lock)", resp.GetAcquired(), err)
	}
}

// TestConcurrencyLimitChangeTakesEffect verifies that creating a limit rule at
// runtime changes the effective limit (F-16): a key that was a lock (limit 1)
// becomes a limit of 2 once a rule is added, so a second job can now be
// admitted.
func TestConcurrencyLimitChangeTakesEffect(t *testing.T) {
	client, _ := startConcurrencyServer(t, nil)
	ctx := context.Background()

	// With no rule, "k" is a lock: the second job is blocked.
	a := createJobWithKeys(t, client, "a", "", []string{"k"})
	b := createJobWithKeys(t, client, "b", "", []string{"k"})
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: a}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job a: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || resp.GetAcquired() {
		t.Fatalf("job b: acquired=%v err=%v, want blocked (k is a lock)", resp.GetAcquired(), err)
	}

	// Add a rule raising "k"'s limit to 2; it takes effect immediately.
	if _, err := client.CreateConcurrencyLimit(ctx, &dbpb.CreateConcurrencyLimitRequest{
		Kind: "key", Pattern: "^k$", Limit: 2,
	}); err != nil {
		t.Fatalf("CreateConcurrencyLimit: %v", err)
	}
	// Now b can be admitted (k's limit is 2 and only a holds it).
	if resp, err := client.AcquireJobSlots(ctx, &dbpb.AcquireJobSlotsRequest{JobId: b}); err != nil || !resp.GetAcquired() {
		t.Fatalf("job b after limit change: acquired=%v err=%v, want acquired", resp.GetAcquired(), err)
	}
}
