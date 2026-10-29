// storage_objects.go — storage object records (F-21.6 storage redesign).
//
// The Database service owns the METADATA of every file the artifacts service
// holds: the storage_objects table is the ledger of what a file IS (kind,
// namespace, display name, size, content hash, package coordinates,
// provenance, activity counters), while the artifacts service is a dumb blob
// store keyed by the record's UID. Callers (today: the API) translate
// (namespace, name) <-> uid at the boundary and drive the two-phase
// lifecycle:
//
//	AllocateStorageObject  reserve a (hidden) record + uid for an inbound upload
//	   ...bytes stream to the artifacts service at blob(uid)...
//	ConfirmStorageObject   settle the record with the OBSERVED size/hash
//
// Visibility: a record is visible to Get/List/Resolve only once Confirmed.
// Allocating over an EXISTING (namespace, name) adopts the provisional row
// (crash recovery for an interrupted upload) and conflicts with a settled
// row (immutability: a filed name is never overwritten). Because at most one
// row ever occupies a coordinate, the table's unique index is plain.
package database

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// staleProvisionalTTL bounds how long an unconfirmed (allocated but never
// confirmed) record lingers before opportunistic pruning sweeps it. Its blob
// (if any) is equally abandoned; the artifacts service garbage-collects
// nothing, and an orphaned blob costs bytes but never correctness.
const staleProvisionalTTL = 24 * time.Hour

// ---------------------------------------------------------------------------
// Conversion & validation
// ---------------------------------------------------------------------------

// toProtoStorageObject converts a storage object model into its proto form.
func toProtoStorageObject(o *models.StorageObject) *dbpb.StorageObject {
	return &dbpb.StorageObject{
		Uid:              o.UID,
		Kind:             o.Kind,
		Namespace:        o.Namespace,
		Name:             o.Name,
		Size:             o.Size,
		ContentSha256:    o.ContentSHA256,
		IsPackage:        o.IsPackage,
		PackageName:      o.PackageName,
		Version:          o.Version,
		IsPreRelease:     o.IsPreRelease,
		UploadedBy:       o.UploadedBy,
		CreatedAt:        timestamppb.New(o.CreatedAt),
		LastDownloadedAt: timestamppb.New(o.LastDownloadedAt),
		DownloadCount:    o.DownloadCount,
		Extras:           o.Extras,
		Confirmed:        o.Confirmed,
	}
}

// storageObjKindNormalizes a kind string to one of the model's kind
// constants, rejecting anything else.
func storageObjKindNormalize(kind string) (string, error) {
	k := strings.ToLower(kind)
	switch k {
	case models.StorageObjKindArtifact, models.StorageObjKindLog:
		return k, nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "invalid storage object kind %q (want %q or %q)", kind, models.StorageObjKindArtifact, models.StorageObjKindLog)
	}
}

// storageObjValidateCoordinates checks the mandatory record coordinates.
func storageObjValidateCoordinates(namespace, name string) error {
	if namespace == "" {
		return status.Error(codes.InvalidArgument, "namespace is required")
	}
	if name == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	if len(namespace) > 255 {
		return status.Error(codes.InvalidArgument, "namespace exceeds 255 characters")
	}
	if len(name) > 512 {
		return status.Error(codes.InvalidArgument, "name exceeds 512 characters")
	}
	return nil
}

// pruneStaleProvisionals sweeps unconfirmed records older than
// staleProvisionalTTL (opportunistic hygiene: called on the allocation path,
// so a crashed uploader's leftovers age out without a dedicated janitor).
// Best-effort: a failure here never affects the caller's operation.
func pruneStaleProvisionals(db *gorm.DB) {
	db.Where("confirmed = ? AND created_at < ?", false, time.Now().Add(-staleProvisionalTTL)).
		Delete(&models.StorageObject{})
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// AllocateStorageObject provisions a record for an upcoming upload (see the
// package comment for the two-phase contract). It returns the adopted or
// freshly-created record, INCLUDING its uid — the caller streams the bytes
// to the artifacts service at blob(uid) and confirms with the observed
// measurements.
func (s *Server) AllocateStorageObject(ctx context.Context, req *dbpb.AllocateStorageObjectRequest) (*dbpb.StorageObject, error) {
	kind, err := storageObjKindNormalize(req.GetKind())
	if err != nil {
		return nil, err
	}
	if err := storageObjValidateCoordinates(req.GetNamespace(), req.GetName()); err != nil {
		return nil, err
	}
	if req.GetIsPackage() && (req.GetPackageName() == "" || req.GetVersion() == "") {
		return nil, status.Error(codes.InvalidArgument, "package filings require package_name and version")
	}
	db := s.db.WithContext(ctx)
	pruneStaleProvisionals(db)

	// Adopt-or-conflict: a row may already occupy the coordinates.
	var obj models.StorageObject
	err = db.Where("namespace = ? AND name = ?", req.GetNamespace(), req.GetName()).First(&obj).Error
	if err == nil {
		if obj.Confirmed && kind != models.StorageObjKindLog {
			return nil, status.Errorf(codes.AlreadyExists,
				"%s %q already exists in namespace %q (filed names are immutable)", kind, req.GetName(), req.GetNamespace())
		}
		// Adopt it (interrupted-upload recovery for artifacts; a log record
		// is normally auto-confirmed at allocation, so this mostly matters
		// for artifacts). Keep its uid (the partially-written blob sits at
		// blob(uid)); refresh the descriptive fields so a re-issue with
		// different coordinates lands consistently.
		obj.Kind = kind
		obj.UploadedBy = req.GetUploadedBy()
		obj.IsPackage = req.GetIsPackage()
		obj.PackageName = req.GetPackageName()
		obj.Version = req.GetVersion()
		obj.IsPreRelease = req.GetIsPreRelease()
		obj.ContentSHA256 = req.GetContentSha256()
		if kind == models.StorageObjKindLog {
			obj.Confirmed = true // logs are readable while growing
		}
		if serr := db.Save(&obj).Error; serr != nil {
			return nil, grpcErr(serr)
		}
		return toProtoStorageObject(&obj), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, grpcErr(err)
	}

	// Fresh allocation. LOGS are auto-confirmed at allocation (size 0): a
	// log must be READABLE WHILE IT IS GROWING (near-live tailing, F-02/F-23)
	// — hiding it until a confirm that may arrive only at the job's terminal
	// status would strand every in-flight read. The two-phase dance earns its
	// keep for IMMUTABLE artifacts (a half-uploaded filing must not be
	// downloadable); for logs the provisional window is worthless.
	autoConfirm := kind == models.StorageObjKindLog
	now := time.Now()
	obj = models.StorageObject{
		UID:           uuid.NewString(),
		Kind:          kind,
		Namespace:     req.GetNamespace(),
		Name:          req.GetName(),
		UploadedBy:    req.GetUploadedBy(),
		IsPackage:     req.GetIsPackage(),
		PackageName:   req.GetPackageName(),
		Version:       req.GetVersion(),
		IsPreRelease:  req.GetIsPreRelease(),
		ContentSHA256: req.GetContentSha256(),
		CreatedAt:     now,
		Confirmed:     autoConfirm,
	}
	if cerr := db.Create(&obj).Error; cerr != nil {
		// Lost a race: another allocator inserted at the same coordinates
		// between our lookup and insert. Re-read and settle adopt-vs-conflict
		// against whatever WON.
		var rival models.StorageObject
		if rerr := db.Where("namespace = ? AND name = ?", req.GetNamespace(), req.GetName()).First(&rival).Error; rerr == nil {
			if rival.Confirmed && kind != models.StorageObjKindLog {
				return nil, status.Errorf(codes.AlreadyExists,
					"%s %q already exists in namespace %q (filed names are immutable)", kind, req.GetName(), req.GetNamespace())
			}
			rival.Kind = kind
			rival.UploadedBy = req.GetUploadedBy()
			rival.IsPackage = req.GetIsPackage()
			rival.PackageName = req.GetPackageName()
			rival.Version = req.GetVersion()
			rival.IsPreRelease = req.GetIsPreRelease()
			rival.ContentSHA256 = req.GetContentSha256()
			if serr := db.Save(&rival).Error; serr != nil {
				return nil, grpcErr(serr)
			}
			return toProtoStorageObject(&rival), nil
		}
		return nil, grpcErr(cerr)
	}
	return toProtoStorageObject(&obj), nil
}

// ConfirmStorageObject settles an allocated record with the OBSERVED size and
// content hash of the stored bytes and makes it visible. Measurement beats
// attestation: the caller SHOULD compare the observed hash against the
// writer's attested one and refuse to confirm on disagreement (corrupt
// ingest); the record simply stores what WAS measured.
func (s *Server) ConfirmStorageObject(ctx context.Context, req *dbpb.ConfirmStorageObjectRequest) (*dbpb.StorageObject, error) {
	if req.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	db := s.db.WithContext(ctx)
	var obj models.StorageObject
	if err := db.Where("uid = ?", req.GetUid()).First(&obj).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "storage object not found")
		}
		return nil, grpcErr(err)
	}
	if obj.Confirmed {
		return nil, status.Error(codes.InvalidArgument, "storage object is already confirmed")
	}
	obj.Size = req.GetSize()
	obj.ContentSHA256 = req.GetContentSha256()
	obj.Confirmed = true
	// Settlement instant = the record's birth (visibility began now).
	obj.CreatedAt = time.Now()
	if err := db.Save(&obj).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoStorageObject(&obj), nil
}

// GetStorageObject returns the VISIBLE record for a (namespace, name).
// Provisional (unconfirmed) records are invisible; a miss returns NotFound.
func (s *Server) GetStorageObject(ctx context.Context, req *dbpb.GetStorageObjectRequest) (*dbpb.StorageObject, error) {
	if req.GetNamespace() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace and name are required")
	}
	var obj models.StorageObject
	err := s.db.WithContext(ctx).
		Where("namespace = ? AND name = ? AND confirmed = ?", req.GetNamespace(), req.GetName(), true).
		First(&obj).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "storage object not found")
		}
		return nil, grpcErr(err)
	}
	return toProtoStorageObject(&obj), nil
}

// ListStorageObjects returns the visible records of a namespace (ALL
// namespaces when namespace is ""), optionally filtered by kind. Ordered by
// created_at descending, then name ascending (newest first, deterministic).
func (s *Server) ListStorageObjects(ctx context.Context, req *dbpb.ListStorageObjectsRequest) (*dbpb.ListStorageObjectsResponse, error) {
	query := s.db.WithContext(ctx).Where("confirmed = ?", true)
	if req.GetNamespace() != "" {
		query = query.Where("namespace = ?", req.GetNamespace())
	}
	if req.GetKind() != "" {
		if _, err := storageObjKindNormalize(req.GetKind()); err != nil {
			return nil, err
		}
		query = query.Where("kind = ?", strings.ToLower(req.GetKind()))
	}
	var objs []models.StorageObject
	if err := query.Order("created_at DESC, name ASC").Find(&objs).Error; err != nil {
		return nil, grpcErr(err)
	}
	out := &dbpb.ListStorageObjectsResponse{}
	for i := range objs {
		out.Objects = append(out.Objects, toProtoStorageObject(&objs[i]))
	}
	return out, nil
}

// TouchStorageObject records a retrieval of a file: increments
// download_count and stamps last_downloaded_at. Analytics only — a miss
// returns NotFound but callers are expected to ignore the error (a download
// is never failed by a failed touch).
func (s *Server) TouchStorageObject(ctx context.Context, req *dbpb.TouchStorageObjectRequest) (*dbpb.StorageObject, error) {
	if req.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	db := s.db.WithContext(ctx)
	res := db.Model(&models.StorageObject{}).
		Where("uid = ? AND confirmed = ?", req.GetUid(), true).
		UpdateColumn("download_count", gorm.Expr("download_count + 1"))
	if res.Error != nil {
		return nil, grpcErr(res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "storage object not found")
	}
	db.Model(&models.StorageObject{}).Where("uid = ?", req.GetUid()).
		UpdateColumn("last_downloaded_at", time.Now())
	var obj models.StorageObject
	if err := db.Where("uid = ?", req.GetUid()).First(&obj).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoStorageObject(&obj), nil
}

// SyncLogSize updates a log record's size WITHOUT affecting its visibility
// (logs grow via the artifacts service's AppendBlob; the API reconciles the
// record so consumers observe the growing size). Non-log records are
// rejected.
func (s *Server) SyncLogSize(ctx context.Context, req *dbpb.SyncLogSizeRequest) (*dbpb.StorageObject, error) {
	if req.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	db := s.db.WithContext(ctx)
	res := db.Model(&models.StorageObject{}).
		Where("uid = ? AND kind = ?", req.GetUid(), models.StorageObjKindLog).
		UpdateColumn("size", req.GetSize())
	if res.Error != nil {
		return nil, grpcErr(res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "log storage object not found")
	}
	var obj models.StorageObject
	if err := db.Where("uid = ?", req.GetUid()).First(&obj).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoStorageObject(&obj), nil
}

// DeleteStorageObject removes a record. The CALLER deletes the blob
// separately through the artifacts service (order-independent: an orphaned
// blob is inert, an orphaned record is NotFound — both directions degrade
// safely).
func (s *Server) DeleteStorageObject(ctx context.Context, req *dbpb.DeleteStorageObjectRequest) (*emptypb.Empty, error) {
	if req.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	res := s.db.WithContext(ctx).Where("uid = ?", req.GetUid()).Delete(&models.StorageObject{})
	if res.Error != nil {
		return nil, grpcErr(res.Error)
	}
	if res.RowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "storage object not found")
	}
	return &emptypb.Empty{}, nil
}

// ---------------------------------------------------------------------------
// Package resolution (F-21.6)
// ---------------------------------------------------------------------------

// resolvePackageRecords resolves ONE spec.artifacts reference against the
// registry RECORDS (pure SQL — no store involvement):
//
//   - use_latest: the newest ELIGIBLE version by upload time (created_at
//     DESC, version DESC as the deterministic tiebreak). The release channel
//     (is_pre_release = false) is eligible by default; allow_pre_release
//     additionally admits flagged records — unrestricted, or constrained to
//     versions matching pre_release_regex when that is set.
//   - pinned: the exact tag; a missing version is a NotFound-shaped error.
//
// Channel membership rests ENTIRELY on the is_pre_release flag stamped at
// publish time — the version string is never inspected for channel hints.
func resolvePackageRecords(db *gorm.DB, req *dbpb.ResolvePackageReferenceRequest) (*dbpb.StorageObject, error) {
	pkg := req.GetPackageName()
	if pkg == "" {
		return nil, status.Error(codes.InvalidArgument, "package_name is required")
	}
	// Every branch builds its query FRESH off db.Model(...) — GORM chained
	// builders MUTATE on .Count()/clones, so a shared base builder poisons
	// later branches ("Table not set").
	base := func() *gorm.DB {
		return db.Model(&models.StorageObject{}).
			Where("confirmed = ? AND is_package = ? AND package_name = ?", true, true, pkg)
	}
	notFoundMsg := func(detail string) error {
		return status.Errorf(codes.NotFound, "package %q: %s", pkg, detail)
	}
	if !req.GetUseLatest() {
		tag := req.GetTag()
		if tag == "" {
			return nil, status.Errorf(codes.InvalidArgument, "package %q: tag is required when use_latest is false", pkg)
		}
		var obj models.StorageObject
		err := base().Where("version = ?", tag).First(&obj).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, notFoundMsg(fmt.Sprintf("has no version %q", tag))
			}
			return nil, grpcErr(err)
		}
		return toProtoStorageObject(&obj), nil
	}
	// use_latest.
	if !req.GetAllowPreRelease() {
		var obj models.StorageObject
		err := base().Where("is_pre_release = ?", false).
			Order("created_at DESC, version DESC").First(&obj).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, notFoundMsg("has no eligible version (use_latest; releases only)")
			}
			return nil, grpcErr(err)
		}
		return toProtoStorageObject(&obj), nil
	}
	if rx := req.GetPreReleaseRegex(); rx != "" {
		re, err := regexp.Compile(rx)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "package %q: pre_release_regex %q does not compile: %v", pkg, rx, err)
		}
		var objs []models.StorageObject
		if err := base().Find(&objs).Error; err != nil {
			return nil, grpcErr(err)
		}
		eligible := make([]*models.StorageObject, 0, len(objs))
		for i := range objs {
			o := &objs[i]
			// The regex constrains the PRE-RELEASE PART of the version (the
			// suffix after the first "-"), per the spec: "^rc" admits
			// 2.0.0-rc.1 but not 2.0.0 (a release, which is always eligible
			// anyway).
			part := o.Version
			if di := strings.IndexByte(o.Version, '-'); di >= 0 {
				part = o.Version[di+1:]
			}
			if !o.IsPreRelease || re.MatchString(part) {
				eligible = append(eligible, o)
			}
		}
		best := latestEligibleRecord(eligible)
		if best == nil {
			return nil, notFoundMsg(fmt.Sprintf("has no eligible version (use_latest; pre-releases matching %q)", rx))
		}
		return toProtoStorageObject(best), nil
	}
	// Pre-releases admitted without restriction: the newest OVERALL (either
	// channel) wins — a single ordered query suffices.
	var obj models.StorageObject
	err := base().Order("created_at DESC, version DESC").First(&obj).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, notFoundMsg("has no eligible version (use_latest; pre-releases included)")
		}
		return nil, grpcErr(err)
	}
	return toProtoStorageObject(&obj), nil
}

// latestEligibleRecord picks the newest record by (created_at DESC, version
// DESC) from an in-memory pool (the regex-filtered branch).
func latestEligibleRecord(recs []*models.StorageObject) *models.StorageObject {
	var best *models.StorageObject
	for _, r := range recs {
		if best == nil {
			best = r
			continue
		}
		if r.CreatedAt.After(best.CreatedAt) ||
			(r.CreatedAt.Equal(best.CreatedAt) && r.Version > best.Version) {
			best = r
		}
	}
	return best
}

// ResolvePackageReference is the RPC face of resolvePackageRecords.
func (s *Server) ResolvePackageReference(ctx context.Context, req *dbpb.ResolvePackageReferenceRequest) (*dbpb.StorageObject, error) {
	return resolvePackageRecords(s.db.WithContext(ctx), req)
}

// resolveRunPackages resolves EACH of a run definition's spec.artifacts
// references against the registry records (F-21.6) and returns the PER-RUN
// SNAPSHOT: reference name -> the winning filing's DISPLAY NAME. The display
// name is stable (filed names are immutable), so a run's jobs download
// STRAIGHT from the registry namespace by that name — no per-run copying.
// Invoked INSIDE the run-creation transaction so the snapshot commits
// atomically with the run row. A resolution failure aborts run creation (a
// run is never created that cannot deliver its declared packages).
func (s *Server) resolveRunPackages(tx *gorm.DB, refs []models.PackageArtifact) (map[string]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	snap := make(map[string]string, len(refs))
	for _, ref := range refs {
		pkg := ref.PackageName
		if pkg == "" {
			pkg = ref.Name
		}
		record, err := resolvePackageRecords(tx, &dbpb.ResolvePackageReferenceRequest{
			PackageName:     pkg,
			Tag:             ref.Tag,
			UseLatest:       ref.UseLatest,
			AllowPreRelease: ref.AllowPreRelease,
			PreReleaseRegex: ref.PreReleaseRegex,
		})
		if err != nil {
			// A MISS (the package or version does not exist) is an operator
			// mistake — surface it as InvalidArgument, DISTINCT from a
			// storage fault (Internal). Other codes propagate as-is.
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				return nil, grpcInvalidArg("resolve package %q (reference %q): %v", pkg, ref.Name, err)
			}
			return nil, err
		}
		snap[ref.Name] = record.GetName()
	}
	return snap, nil
}

// grpcInvalidArg wraps a message as an InvalidArgument status (used by
// run-creation paths that must surface operator mistakes distinctly from
// storage faults).
func grpcInvalidArg(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}
