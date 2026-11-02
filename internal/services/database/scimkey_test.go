package database

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cdrom/internal/models"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// mkScimKeyReq builds a CreateScimKeyRequest with a distinct pseudo-encoding
// (shape-checked, not KDF-verified — this suite tests the pool mechanics, and
// the IdP's derivation is covered in internal/idp).
func mkScimKeyReq(desc string) *dbpb.CreateScimKeyRequest {
	return &dbpb.CreateScimKeyRequest{
		Key: &dbpb.ScimKey{
			Hash:        "$argon2id$v=19,m=8192,t=2,p=1$" + desc + "=pseudo-encoding",
			Prefix:      "abcd1234",
			Description: desc,
			CreatedBy:   "tester",
		},
	}
}

// TestScimKeyPoolCRUD exercises the F-28.2 pool fundamentals against a real
// SQLite-backed Database service: create/get/list, newest-first ordering,
// the exclusion of the encoding from reads/lists (bundle-only material), and
// the five-key cap.
func TestScimKeyPoolCRUD(t *testing.T) {
	client := startServer(t)
	ctx := context.Background()

	// Create three keys.
	var ids []string
	for _, desc := range []string{"alpha", "beta", "gamma"} {
		created, err := client.CreateScimKey(ctx, mkScimKeyReq(desc))
		if err != nil {
			t.Fatalf("CreateScimKey(%s): %v", desc, err)
		}
		ids = append(ids, created.GetId())
		if created.GetHash() != "" {
			t.Errorf("create response leaks the encoding: %q", created.GetHash())
		}
		if created.GetDescription() != desc {
			t.Errorf("description = %q, want %q", created.GetDescription(), desc)
		}
	}

	// Get returns the row (again without the encoding).
	got, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: ids[1]})
	if err != nil {
		t.Fatalf("GetScimKey: %v", err)
	}
	if got.GetDescription() != "beta" {
		t.Errorf("get description = %q, want beta", got.GetDescription())
	}
	if got.GetHash() != "" {
		t.Errorf("get response leaks the encoding: %q", got.GetHash())
	}

	// List (active only) returns all three, newest first.
	list, err := client.ListScimKeys(ctx, &dbpb.ListScimKeysRequest{})
	if err != nil {
		t.Fatalf("ListScimKeys: %v", err)
	}
	if len(list.GetKeys()) != 3 {
		t.Fatalf("list returned %d keys, want 3", len(list.GetKeys()))
	}
	wantDescs := []string{"gamma", "beta", "alpha"}
	for i, k := range list.GetKeys() {
		if k.GetDescription() != wantDescs[i] {
			t.Errorf("list[%d] = %q, want %q (newest first)", i, k.GetDescription(), wantDescs[i])
		}
	}

	// The cap: two more fit (total 5), the sixth is AlreadyExists.
	if _, err := client.CreateScimKey(ctx, mkScimKeyReq("delta")); err != nil {
		t.Fatalf("CreateScimKey(delta): %v", err)
	}
	if _, err := client.CreateScimKey(ctx, mkScimKeyReq("epsilon")); err != nil {
		t.Fatalf("CreateScimKey(epsilon): %v", err)
	}
	_, err = client.CreateScimKey(ctx, mkScimKeyReq("zeta"))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("sixth create: code = %v, want AlreadyExists (cap of %d)", status.Code(err), scimPoolCap)
	}

	// Unknown id is NotFound.
	if _, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: "999999"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing get: code = %v, want NotFound", status.Code(err))
	}
	if _, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: "abc"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed get: code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestScimKeyValidityTransitions exercises the F-28.2 lifecycle: revoke
// (instant, CAS), reinstate (within the 24 h window), the CAS honesty of a
// double revoke, the aged-reinstate refusal, and the prune sweep.
func TestScimKeyValidityTransitions(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	created, err := client.CreateScimKey(ctx, mkScimKeyReq("lifecycler"))
	if err != nil {
		t.Fatalf("CreateScimKey: %v", err)
	}
	id := created.GetId()

	// Fresh keys have no revocation stamp.
	fresh, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: id})
	if err != nil {
		t.Fatalf("GetScimKey: %v", err)
	}
	if fresh.GetRevokedAt() != nil {
		t.Fatalf("fresh key unexpectedly revoked: %v", fresh.GetRevokedAt())
	}

	// Revoke: the stamp lands, and the key disappears from the active list.
	revo, err := client.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: id})
	if err != nil {
		t.Fatalf("RevokeScimKey: %v", err)
	}
	if revo.GetRevokedAt() == nil {
		t.Fatal("revoked key carries no revoked_at")
	}
	active, err := client.ListScimKeys(ctx, &dbpb.ListScimKeysRequest{})
	if err != nil {
		t.Fatalf("ListScimKeys(active): %v", err)
	}
	if len(active.GetKeys()) != 0 {
		t.Fatalf("active list still shows the revoked key (%d entries)", len(active.GetKeys()))
	}
	// ...but it is readable while inside the reinstate window.
	within, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: id})
	if err != nil {
		t.Fatalf("GetScimKey(within window): %v", err)
	}
	if within.GetRevokedAt() == nil {
		t.Fatal("within-window read lost the revocation stamp")
	}

	// Double revoke: CAS honesty — Conflict, never a silent success.
	if _, err := client.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: id}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("double revoke: code = %v, want Conflict", status.Code(err))
	}

	// Reinstate: the stamp clears and the key rejoins the active list.
	inst, err := client.ReinstateScimKey(ctx, &dbpb.ReinstateScimKeyRequest{Id: id})
	if err != nil {
		t.Fatalf("ReinstateScimKey: %v", err)
	}
	if inst.GetRevokedAt() != nil {
		t.Fatalf("reinstated key still revoked: %v", inst.GetRevokedAt())
	}
	active, err = client.ListScimKeys(ctx, &dbpb.ListScimKeysRequest{})
	if err != nil {
		t.Fatalf("ListScimKeys(after reinstate): %v", err)
	}
	if len(active.GetKeys()) != 1 {
		t.Fatalf("active list after reinstate = %d entries, want 1", len(active.GetKeys()))
	}

	// Reinstate of a non-revoked key: Conflict (nothing to recover).
	if _, err := client.ReinstateScimKey(ctx, &dbpb.ReinstateScimKeyRequest{Id: id}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reinstate(non-revoked): code = %v, want Conflict", status.Code(err))
	}

	// Age the revocation BEYOND the window (simulating 25 h) and confirm
	// reinstate refuses while prune finally removes the row.
	_, err = client.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: id})
	if err != nil {
		t.Fatalf("re-revoke: %v", err)
	}
	old := time.Now().Add(-(25 * time.Hour))
	if err := db.Model(&models.ScimKey{}).Where("id = ?", id).
		UpdateColumn("revoked_at", old).Error; err != nil {
		t.Fatalf("age the revocation: %v", err)
	}
	if _, err := client.ReinstateScimKey(ctx, &dbpb.ReinstateScimKeyRequest{Id: id}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("aged reinstate: code = %v, want Conflict (window elapsed)", status.Code(err))
	}
	pruned, err := client.PruneScimKeys(ctx, &dbpb.PruneScimKeysRequest{Cutoff: durationpb.New(24 * time.Hour)})
	if err != nil {
		t.Fatalf("PruneScimKeys: %v", err)
	}
	if pruned.GetDeleted() != 1 {
		t.Fatalf("prune deleted %d rows, want 1", pruned.GetDeleted())
	}
	if _, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: id}); status.Code(err) != codes.NotFound {
		t.Fatalf("pruned get: code = %v, want NotFound", status.Code(err))
	}
	// Prune is idempotent (nothing left to delete).
	again, err := client.PruneScimKeys(ctx, &dbpb.PruneScimKeysRequest{Cutoff: durationpb.New(24 * time.Hour)})
	if err != nil {
		t.Fatalf("PruneScimKeys (2): %v", err)
	}
	if again.GetDeleted() != 0 {
		t.Fatalf("second prune deleted %d rows, want 0", again.GetDeleted())
	}
}

// TestScimKeyUpdateSemantics checks the F-28.2 partial-update contract:
// description swap, expiration set, explicit expiration CLEAR (NULL), the
// frozen identity columns, and the refuse-edits-while-revoked rule.
func TestScimKeyUpdateSemantics(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	created, err := client.CreateScimKey(ctx, mkScimKeyReq("editable"))
	if err != nil {
		t.Fatalf("CreateScimKey: %v", err)
	}
	id := created.GetId()

	// Swap the description only.
	upd, err := client.UpdateScimKey(ctx, &dbpb.UpdateScimKeyRequest{
		Id: id, Description: "renamed", HasDescription: true,
	})
	if err != nil {
		t.Fatalf("UpdateScimKey(description): %v", err)
	}
	if upd.GetDescription() != "renamed" {
		t.Fatalf("description = %q, want renamed", upd.GetDescription())
	}
	if upd.GetExpiresAt() != nil {
		t.Fatalf("touching the description perturbed expires_at: %v", upd.GetExpiresAt())
	}

	// Set an expiration.
	exp := time.Now().UTC().Add(72 * time.Hour)
	upd, err = client.UpdateScimKey(ctx, &dbpb.UpdateScimKeyRequest{
		Id: id, ExpiresAt: timestamppb.New(exp), HasExpiresAt: true,
	})
	if err != nil {
		t.Fatalf("UpdateScimKey(set expiry): %v", err)
	}
	if upd.GetExpiresAt() == nil || !upd.GetExpiresAt().AsTime().Equal(exp) {
		t.Fatalf("expires_at = %v, want %v", upd.GetExpiresAt(), exp)
	}

	// Clear it again (explicit NULL).
	upd, err = client.UpdateScimKey(ctx, &dbpb.UpdateScimKeyRequest{
		Id: id, ClearExpiresAt: true,
	})
	if err != nil {
		t.Fatalf("UpdateScimKey(clear expiry): %v", err)
	}
	if upd.GetExpiresAt() != nil {
		t.Fatalf("clear left expires_at set: %v", upd.GetExpiresAt())
	}
	var row models.ScimKey
	if err := db.Where("id = ?", id).First(&row).Error; err != nil {
		t.Fatalf("load row: %v", err)
	}
	if row.ExpiresAt != nil {
		t.Fatalf("storage still holds an expiration: %v", *row.ExpiresAt)
	}

	// Editing a revoked key is refused (reinstate first).
	if _, err := client.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: id}); err != nil {
		t.Fatalf("RevokeScimKey: %v", err)
	}
	if _, err := client.UpdateScimKey(ctx, &dbpb.UpdateScimKeyRequest{
		Id: id, Description: "late", HasDescription: true,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("update(revoked): code = %v, want Conflict", status.Code(err))
	}
}

// TestScimKeyBundleMaterial checks the F-28.2 central-candidacy contract: the
// bundle carries the ENCODINGS (validation material) of exactly the
// non-revoked rows — however old (no automatic ageing-out) — while ordinary
// reads never do.
func TestScimKeyBundleMaterial(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	young, err := client.CreateScimKey(ctx, mkScimKeyReq("young"))
	if err != nil {
		t.Fatalf("CreateScimKey(young): %v", err)
	}
	elder, err := client.CreateScimKey(ctx, mkScimKeyReq("elder"))
	if err != nil {
		t.Fatalf("CreateScimKey(elder): %v", err)
	}
	zombie, err := client.CreateScimKey(ctx, mkScimKeyReq("zombie"))
	if err != nil {
		t.Fatalf("CreateScimKey(zombie): %v", err)
	}
	if _, err := client.RevokeScimKey(ctx, &dbpb.RevokeScimKeyRequest{Id: zombie.GetId()}); err != nil {
		t.Fatalf("RevokeScimKey(zombie): %v", err)
	}

	// Age the elder ten YEARS: a non-revoked key never ages out of candidacy.
	tenYearsAgo := time.Now().Add(-10 * 365 * 24 * time.Hour)
	if err := db.Model(&models.ScimKey{}).Where("id = ?", elder.GetId()).
		Updates(map[string]any{"created_at": tenYearsAgo}).Error; err != nil {
		t.Fatalf("age the elder: %v", err)
	}

	bundle, err := client.BundleScimKeys(ctx, &dbpb.BundleScimKeysRequest{})
	if err != nil {
		t.Fatalf("BundleScimKeys: %v", err)
	}
	seen := map[string]bool{}
	for _, k := range bundle.GetKeys() {
		if k.GetHash() == "" {
			t.Fatalf("bundle entry %s lacks its encoding (validation material)", k.GetId())
		}
		seen[k.GetId()] = true
	}
	if len(bundle.GetKeys()) != 2 || !seen[young.GetId()] || !seen[elder.GetId()] {
		t.Fatalf("bundle = %+v, want exactly the two non-revoked keys (incl. the decade-old one)", bundle.GetKeys())
	}
	if seen[zombie.GetId()] {
		t.Fatal("bundle includes a REVOKED key")
	}

	// Ordinary reads withhold the encoding.
	read, err := client.GetScimKey(ctx, &dbpb.GetScimKeyRequest{Id: elder.GetId()})
	if err != nil {
		t.Fatalf("GetScimKey(elder): %v", err)
	}
	if read.GetHash() != "" {
		t.Fatalf("ordinary read leaks the encoding: %q", read.GetHash())
	}
}

// TestScimKeyMarkUsedThrottle checks the write-throttled last_used_at bump:
// the first mark lands, a rapid second is suppressed, and a mark after the
// throttle window lands again.
func TestScimKeyMarkUsedThrottle(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	created, err := client.CreateScimKey(ctx, mkScimKeyReq("usage"))
	if err != nil {
		t.Fatalf("CreateScimKey: %v", err)
	}
	id := created.GetId()

	if _, err := client.MarkScimKeyUsed(ctx, &dbpb.MarkScimKeyUsedRequest{
		Id: id, Throttle: durationpb.New(time.Minute),
	}); err != nil {
		t.Fatalf("MarkScimKeyUsed (1): %v", err)
	}
	var row models.ScimKey
	if err := db.Where("id = ?", id).First(&row).Error; err != nil {
		t.Fatalf("load row: %v", err)
	}
	if row.LastUsedAt == nil {
		t.Fatal("first mark did not bump last_used_at")
	}
	first := *row.LastUsedAt

	// Immediate second mark: throttled (no-op).
	if _, err := client.MarkScimKeyUsed(ctx, &dbpb.MarkScimKeyUsedRequest{
		Id: id, Throttle: durationpb.New(time.Minute),
	}); err != nil {
		t.Fatalf("MarkScimKeyUsed (2): %v", err)
	}
	if err := db.Where("id = ?", id).First(&row).Error; err != nil {
		t.Fatalf("load row (2): %v", err)
	}
	if row.LastUsedAt == nil || !row.LastUsedAt.Equal(first) {
		t.Fatalf("rapid second mark bumped last_used_at again (%v → %v)", first, row.LastUsedAt)
	}

	// Force the clock story: pretend the last use was two minutes ago, then
	// mark again — it must land.
	backdated := time.Now().Add(-2 * time.Minute)
	if err := db.Model(&models.ScimKey{}).Where("id = ?", id).
		UpdateColumn("last_used_at", backdated).Error; err != nil {
		t.Fatalf("backdate last_used_at: %v", err)
	}
	if _, err := client.MarkScimKeyUsed(ctx, &dbpb.MarkScimKeyUsedRequest{
		Id: id, Throttle: durationpb.New(time.Minute),
	}); err != nil {
		t.Fatalf("MarkScimKeyUsed (3): %v", err)
	}
	if err := db.Where("id = ?", id).First(&row).Error; err != nil {
		t.Fatalf("load row (3): %v", err)
	}
	if row.LastUsedAt == nil || !row.LastUsedAt.After(backdated) {
		t.Fatalf("third mark did not advance last_used_at (have %v)", row.LastUsedAt)
	}
}

// TestScimKeyEncodingStoredOpaque checks the F-28.2 storage guarantee: the
// encoding the caller supplied is persisted BYTE-FOR-BYTE (the database is
// opaque to it — no normalization, truncation, or interpretation).
func TestScimKeyEncodingStoredOpaque(t *testing.T) {
	client, db := startServerDB(t)
	ctx := context.Background()

	const enc = "$argon2id$v=19,m=8192,t=2,p=1$c2FsdHNhbHQ$VGFnVGFnVGFn"
	created, err := client.CreateScimKey(ctx, &dbpb.CreateScimKeyRequest{
		Key: &dbpb.ScimKey{Hash: enc, Prefix: "abcdefgh", Description: "opaque"},
	})
	if err != nil {
		t.Fatalf("CreateScimKey: %v", err)
	}
	var row models.ScimKey
	if err := db.Where("id = ?", created.GetId()).First(&row).Error; err != nil {
		t.Fatalf("load row: %v", err)
	}
	if row.Hash != enc {
		t.Fatalf("stored encoding altered: have %q want %q", row.Hash, enc)
	}
	if row.Prefix != "abcdefgh" {
		t.Fatalf("stored prefix altered: %q", row.Prefix)
	}

	// The unique index on the encoding guards against duplicate material.
	_, dupErr := client.CreateScimKey(ctx, &dbpb.CreateScimKeyRequest{
		Key: &dbpb.ScimKey{Hash: enc, Prefix: "xy", Description: "dup"},
	})
	if dupErr == nil {
		t.Fatal("duplicate encoding accepted (want the unique index to refuse)")
	}
	if status.Code(dupErr) == codes.OK {
		t.Fatalf("duplicate encoding: unexpected OK code with error %v", dupErr)
	}
}
