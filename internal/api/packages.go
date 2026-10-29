// packages.go — the package registry HTTP surface (F-21.6, storage-redesign
// edition): publishing a version into the reserved "packages" namespace,
// browsing/downloading the registry, and promoting a file between namespaces
// (the former F-19 operation).
//
// Topology: the API relays all store traffic (targets and humans talk only to
// the API). Publishing happens from CI hosts and laptops (user-authenticated
// HTTP); jobs publish through the publish_artifact STEP handler into their
// own (job-id) namespace via the gRPC artifact proxy — not through these
// endpoints.
//
// Storage (F-21.6 storage redesign): a filing is a storage_object RECORD
// (owned by the Database service) whose coordinates (package_name, version,
// is_pre_release) are EXPlicit columns — the pre-release channel is a flag
// set AT PUBLISH TIME, never parsed from the version string. The bytes sit in
// the blob service at the record's uid. Publication is two-phase: allocate
// the record (reserving the coordinate), stream the bytes to the blob while
// measuring the SHA-256, then confirm the record with the OBSERVED size and
// the measured hash. Browse/list read the records directly (no store walks,
// no filename parsing).
package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cdrom/internal/audit"
	"cdrom/internal/auth"
	"cdrom/internal/authz"
	artifactspb "cdrom/internal/gen/cdrom/artifacts/v1"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/services/artifacts"
	"cdrom/internal/services/database"
)

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

// publishPackageHeader is the JSON preamble of a publish request. The body
// format is a header LINE (newline-terminated) followed by the raw package
// bytes — scripting-friendly (pipe a file in after a crafted header line) and
// unambiguous.
type publishPackageHeader struct {
	// Name is the package name (required).
	Name string `json:"name"`
	// Version is the version/tag (required).
	Version string `json:"version"`
	// Ext is the filing extension (optional; defaults to ".zip"). Multi-part
	// extensions (".tar.gz") are supported.
	Ext string `json:"ext,omitempty"`
	// Prerelease EXPLICITLY places the filing on the pre-release channel
	// (F-21.6 storage redesign): resolution excludes flagged filings from
	// the release channel unless a reference opts in. Default false. The
	// flag is the publisher's assertion — it is NEVER inferred from the
	// version string.
	Prerelease bool `json:"prerelease,omitempty"`
}

// publishPackage handles POST /api/packages: publishes a package version into
// the reserved registry namespace. Requires packages.can-publish. Filings
// are IMMUTABLE: a colliding (package, version) is rejected (409), never
// overwritten. The API meters the SHA-256 of the ingested bytes AS THEY
// STREAM to the blob (tee-ing: no full-body buffering) and confirms the
// record with the observed size and the measured hash.
func (s *Server) publishPackage(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPackagesPublish, authz.Resource{}) {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, http.StatusBadRequest, "read body: %v", err)
		return
	}
	nl := bytes.IndexByte(body, '\n')
	if nl < 0 {
		httpError(w, http.StatusBadRequest, "body must be a JSON header line followed by the package bytes")
		return
	}
	var hdr publishPackageHeader
	if err := json.Unmarshal(body[:nl], &hdr); err != nil {
		httpError(w, http.StatusBadRequest, "invalid header: %v", err)
		return
	}
	if hdr.Name == "" || hdr.Version == "" {
		httpError(w, http.StatusBadRequest, "header requires name and version")
		return
	}
	ext := hdr.Ext
	if ext == "" {
		ext = ".zip"
	}
	displayName := artifacts.ComposePackageNameVersion(hdr.Name, hdr.Version, ext)
	// Coordinate checks (charset + round-trip) — the single source of truth
	// shared with the database service.
	if err := database.ValidatePublicationChecks(hdr.Name, hdr.Version, ext); err != nil {
		httpError(w, http.StatusBadRequest, "%v", err)
		return
	}
	payload := body[nl+1:]
	sum := sha256.Sum256(payload)
	attestedSHA := hex.EncodeToString(sum[:])

	// Immutability: a VISIBLE filing at the same coordinate is rejected (409).
	if _, gerr := s.resolver.getFile(r.Context(), database.PackageRegistryNamespace, displayName); gerr == nil {
		httpError(w, http.StatusConflict, "package version %q already exists (registry versions are immutable)", displayName)
		return
	} else if st, ok := status.FromError(gerr); !ok || st.Code() != codes.NotFound {
		grpcError(w, gerr)
		return
	}

	// PHASE 1: allocate the record (reserve the coordinate, get the uid).
	// Adoption semantics: a sibling's INTERRUPTED filing at the same
	// coordinate is adopted (its provisional record is returned), so a
	// re-issue converges on the same blob.
	rec, err := s.resolver.allocate(r.Context(), &dbpb.AllocateStorageObjectRequest{
		Kind:          "artifact",
		Namespace:     database.PackageRegistryNamespace,
		Name:          displayName,
		UploadedBy:    auth.UserFromContext(r.Context()).Subject,
		IsPackage:     true,
		PackageName:   hdr.Name,
		Version:       hdr.Version,
		IsPreRelease:  hdr.Prerelease,
		ContentSha256: attestedSHA,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			httpError(w, http.StatusConflict, "package version %q already exists (registry versions are immutable)", displayName)
			return
		}
		grpcError(w, err)
		return
	}

	// PHASE 2: stream the bytes to the blob at the record's uid, METERING
	// the hash as they flow (the API is the meter of record). Clear any
	// PARTIAL blob a previous interrupted issuance left at this uid (the
	// allocation adopts the sibling's record; the blob may linger non-empty,
	// and a put to a non-empty id fails) — a no-op when the blob is absent.
	_, _ = s.clients.Artifacts.DeleteBlob(r.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
	put, err := s.clients.Artifacts.PutBlob(r.Context())
	if err != nil {
		grpcError(w, err)
		return
	}
	measured, size, err := pumpUploadBody(put, rec.GetUid(), bytes.NewReader(payload))
	if err != nil {
		// Interrupted upload: abandon the partial blob (the record stays
		// provisional and is adoptable by a re-issue).
		_, _ = s.clients.Artifacts.DeleteBlob(r.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
		grpcError(w, err)
		return
	}
	// Integrity: the measured hash must equal the attested one (payload was
	// read from the same buffer, so a mismatch indicates a corrupted stream).
	if measured != attestedSHA {
		_, _ = s.clients.Artifacts.DeleteBlob(r.Context(), &artifactspb.BlobRef{Id: rec.GetUid()})
		httpError(w, http.StatusInternalServerError, "ingest integrity check failed (measured %s ≠ attested %s)", measured, attestedSHA)
		return
	}

	// PHASE 3: confirm the record with the OBSERVEd measurements.
	confirmed, err := s.resolver.confirm(r.Context(), rec.GetUid(), size, measured)
	if err != nil {
		grpcError(w, err)
		return
	}
	createdAt := ""
	if ts := confirmed.GetCreatedAt(); ts != nil {
		createdAt = ts.AsTime().UTC().Format(time.RFC3339)
	}
	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPackagePublish,
		TargetKind: audit.TargetPackage,
		TargetName: displayName,
		Outcome:    audit.OutcomeSuccess,
		Details: auditJSON(map[string]any{
			"name":       hdr.Name,
			"version":    hdr.Version,
			"prerelease": hdr.Prerelease,
			"size":       size,
			"sha256":     measured,
		}),
	})
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":         confirmed.GetUid(),
		"name":       hdr.Name,
		"version":    hdr.Version,
		"filename":   displayName,
		"prerelease": hdr.Prerelease,
		"size":       size,
		"sha256":     measured,
		"createdAt":  createdAt,
	})
}

// pumpUploadBody relays a LOCAL reader into a PutBlob client stream seeded
// with uid, measuring the SHA-256 of the bytes as they flow. Returns the
// measured hash and the blob-reported stored size. (The gRPC proxy's
// pumpUpload does the analogous job for a REMOTE target stream — the two
// differ in their source type, so they are siblings, not sharers.)
func pumpUploadBody(put grpc.ClientStreamingClient[artifactspb.BlobChunk, artifactspb.BlobStat], uid string, r io.Reader) (string, int64, error) {
	h := sha256.New()
	buf := make([]byte, 256<<10)
	if err := put.Send(&artifactspb.BlobChunk{Id: uid}); err != nil {
		return "", 0, err
	}
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if err := put.Send(&artifactspb.BlobChunk{Data: buf[:n]}); err != nil {
				return "", 0, err
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", 0, rerr
		}
	}
	stat, err := put.CloseAndRecv()
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), stat.GetSize(), nil
}

// ---------------------------------------------------------------------------
// Browsing
// ---------------------------------------------------------------------------

// packageDescriptor is the registry's view of one published version (READ
// FROM THE RECORD — the coordinates are columns, not parsed strings).
type packageDescriptor struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Version    string `json:"version"`
	Filename   string `json:"filename"`
	Prerelease bool   `json:"prerelease"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256,omitempty"`
	Downloads  int64  `json:"downloads"`
	CreatedAt  string `json:"createdAt,omitempty"`
}

// registryDescriptors lists the registry's VISIBLE filings (records with
// is_package) as descriptors. No store walk, no filename parsing.
func (s *Server) registryDescriptors(r *http.Request) ([]packageDescriptor, error) {
	objects, err := s.resolver.listFiles(r.Context(), database.PackageRegistryNamespace, "artifact")
	if err != nil {
		return nil, err
	}
	out := make([]packageDescriptor, 0, len(objects))
	for _, o := range objects {
		if !o.GetIsPackage() {
			continue // a foreign artifact parked in the namespace (shouldn't happen)
		}
		d := packageDescriptor{
			ID:         o.GetUid(),
			Name:       o.GetPackageName(),
			Version:    o.GetVersion(),
			Filename:   o.GetName(),
			Prerelease: o.GetIsPreRelease(),
			Size:       o.GetSize(),
			SHA256:     o.GetContentSha256(),
			Downloads:  o.GetDownloadCount(),
		}
		if ts := o.GetCreatedAt(); ts != nil {
			d.CreatedAt = ts.AsTime().UTC().Format(time.RFC3339)
		}
		out = append(out, d)
	}
	return out, nil
}

// listPackages handles GET /api/packages: the registry grouped by package
// name (each with its versions, newest first). Requires packages.can-view.
func (s *Server) listPackages(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPackagesView, authz.Resource{}) {
		return
	}
	descriptors, err := s.registryDescriptors(r)
	if err != nil {
		grpcError(w, err)
		return
	}
	byName := map[string][]packageDescriptor{}
	for _, d := range descriptors {
		byName[d.Name] = append(byName[d.Name], d)
	}
	out := map[string][]packageDescriptor{}
	for name, descs := range byName {
		newestFirstDescs(descs)
		out[name] = descs
	}
	writeJSON(w, http.StatusOK, out)
}

// getPackage handles GET /api/packages/{name}: one package's descriptors,
// newest first. Requires packages.can-view.
func (s *Server) getPackage(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPackagesView, authz.Resource{}) {
		return
	}
	name := r.PathValue("name")
	all, err := s.registryDescriptors(r)
	if err != nil {
		grpcError(w, err)
		return
	}
	var descs []packageDescriptor
	for _, d := range all {
		if d.Name == name {
			descs = append(descs, d)
		}
	}
	if len(descs) == 0 {
		httpError(w, http.StatusNotFound, "package %q not found in the registry", name)
		return
	}
	newestFirstDescs(descs)
	writeJSON(w, http.StatusOK, descs)
}

// getPackageContents handles GET /api/packages/{name}/{version}/contents:
// streams the version's bytes back. Requires packages.can-view. The filing's
// record is located by (package, version) — the coordinates are COLUMNS, so
// no name scanning is involved.
func (s *Server) getPackageContents(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPackagesView, authz.Resource{}) {
		return
	}
	name, version := r.PathValue("name"), r.PathValue("version")
	all, err := s.registryDescriptors(r)
	if err != nil {
		grpcError(w, err)
		return
	}
	var winner *packageDescriptor
	for i := range all {
		if all[i].Name == name && all[i].Version == version {
			winner = &all[i]
			break
		}
	}
	if winner == nil {
		httpError(w, http.StatusNotFound, "package %q has no version %q in the registry", name, version)
		return
	}
	stream, err := s.clients.Artifacts.GetBlob(r.Context(), &artifactspb.BlobRef{Id: winner.ID})
	if err != nil {
		grpcError(w, err)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, winner.Filename))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}
		if _, werr := w.Write(chunk.GetData()); werr != nil {
			return
		}
	}
	// Retrieveval analytics (best-effort).
	s.resolver.touch(r.Context(), winner.ID)
}

// newestFirstDescs orders descriptors by created-at descending (filename
// descending as a deterministic tiebreak).
func newestFirstDescs(ds []packageDescriptor) {
	for i := 0; i < len(ds); i++ {
		for j := i + 1; j < len(ds); j++ {
			if ds[j].CreatedAt > ds[i].CreatedAt ||
				(ds[j].CreatedAt == ds[i].CreatedAt && ds[j].Filename > ds[i].Filename) {
				ds[i], ds[j] = ds[j], ds[i]
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Promotion (former F-19)
// ---------------------------------------------------------------------------

// promoteArtifactRequestBody is the JSON body of a promotion request.
type promoteArtifactRequestBody struct {
	// SourceNamespace is the store namespace the file is COPIED FROM
	// (a job-id namespace, a run-id namespace, or "packages").
	SourceNamespace string `json:"source_namespace"`
	// SourceName is the file's display name within SourceNamespace.
	SourceName string `json:"source_name"`
	// DestNamespace is the store namespace the file is COPIED INTO.
	DestNamespace string `json:"dest_namespace"`
	// DestName is the file's display name within DestNamespace; empty means
	// the same display name.
	DestName string `json:"dest_name,omitempty"`
	// Promoting INTO the registry requires the package coordinates (the
	// destination becomes a filing): package_name, version, and the
	// EXPLICIT prerelease channel flag.
	PackageName string `json:"package_name,omitempty"`
	Version     string `json:"version,omitempty"`
	Prerelease  bool   `json:"prerelease,omitempty"`
}

// promoteArtifact handles POST /api/artifacts/promote: a SERVER-SIDE COPY of
// a file between two store namespaces (the former F-19 operation). Requires
// packages.can-promote. Byte-integrity is STRUCTURAL: the copy replicates
// the SOURCE BLOB (the artifacts service's CopyBlob), so the destination is
// bit-identical to the source by construction — no re-metering, no
// comparison. Promoting into the registry allocates a filing record with the
// promoted coordinates (package_name/version/prerelease REQUIRED); the
// inherited size and hash come from the source record.
func (s *Server) promoteArtifact(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermPackagesPromote, authz.Resource{}) {
		return
	}
	var req promoteArtifactRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid body: %v", err)
		return
	}
	if req.SourceNamespace == "" || req.SourceName == "" || req.DestNamespace == "" {
		httpError(w, http.StatusBadRequest, "source_namespace, source_name, and dest_namespace are required")
		return
	}
	dstName := req.DestName
	if dstName == "" {
		dstName = req.SourceName
	}

	// Locate the SOURCE record (translate the coordinate).
	srcRec, err := s.resolver.getFile(r.Context(), req.SourceNamespace, req.SourceName)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			httpError(w, http.StatusNotFound, "source file %q/%q not found", req.SourceNamespace, req.SourceName)
			return
		}
		grpcError(w, err)
		return
	}

	isDestRegistry := req.DestNamespace == database.PackageRegistryNamespace
	if isDestRegistry {
		if req.PackageName == "" || req.Version == "" {
			httpError(w, http.StatusBadRequest, "promoting into the registry requires package_name and version")
			return
		}
		if err := database.ValidatePublicationChecks(req.PackageName, req.Version, ""); err != nil {
			httpError(w, http.StatusBadRequest, "%v", err)
			return
		}
		// Inherit the source's extension (a promoted filing keeps its shape,
		// e.g. a .tar.gz stays a .tar.gz); default to ".zip" when the source
		// name carries none.
		srcExt := ""
		if idx := strings.LastIndex(srcRec.GetName(), "."); idx > 0 {
			srcExt = srcRec.GetName()[idx:]
		}
		dstName = artifacts.ComposePackageNameVersion(req.PackageName, req.Version, srcExt)
	}

	// Destination immutability: a VISIBLE filing at the destination
	// coordinate is rejected (409).
	if _, gerr := s.resolver.getFile(r.Context(), req.DestNamespace, dstName); gerr == nil {
		httpError(w, http.StatusConflict, "destination %q/%q already exists (filed names are immutable)", req.DestNamespace, dstName)
		return
	} else if st, ok := status.FromError(gerr); !ok || st.Code() != codes.NotFound {
		grpcError(w, gerr)
		return
	}

	// Allocate the DESTINATION record (inherit the source's hash — the copy
	// is bit-identical by construction, so the source's measured hash IS the
	// destination's).
	destAttr := &dbpb.AllocateStorageObjectRequest{
		Kind:       "artifact",
		Namespace:  req.DestNamespace,
		Name:       dstName,
		UploadedBy: auth.UserFromContext(r.Context()).Subject,
	}
	if isDestRegistry {
		destAttr.IsPackage = true
		destAttr.PackageName = req.PackageName
		destAttr.Version = req.Version
		destAttr.IsPreRelease = req.Prerelease
	}
	destAttr.ContentSha256 = srcRec.GetContentSha256()
	dstRec, err := s.resolver.allocate(r.Context(), destAttr)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
			httpError(w, http.StatusConflict, "destination %q/%q already exists (filed names are immutable)", req.DestNamespace, dstName)
			return
		}
		grpcError(w, err)
		return
	}

	// Replicate the blob SERVER-SIDE (no bytes pass through the API).
	stat, err := s.clients.Artifacts.CopyBlob(r.Context(), &artifactspb.CopyBlobRequest{
		Src: srcRec.GetUid(),
		Dst: dstRec.GetUid(),
	})
	if err != nil {
		// Abort: remove the partial destination blob (the record stays
		// provisional and is adoptable by a retry).
		_, _ = s.clients.Artifacts.DeleteBlob(r.Context(), &artifactspb.BlobRef{Id: dstRec.GetUid()})
		grpcError(w, err)
		return
	}

	// Confirm the destination record (INHERITED measurements).
	confirmed, err := s.resolver.confirm(r.Context(), dstRec.GetUid(), stat.GetSize(), srcRec.GetContentSha256())
	if err != nil {
		grpcError(w, err)
		return
	}

	s.recordAudit(r, audit.Event{
		Action:     audit.ActionPackagePromote,
		TargetKind: audit.TargetPackage,
		TargetName: dstName,
		Outcome:    audit.OutcomeSuccess,
		Details: auditJSON(map[string]any{
			"source_namespace": req.SourceNamespace,
			"source_name":      req.SourceName,
			"dest_namespace":   req.DestNamespace,
			"dest_name":        dstName,
			"sha256":           srcRec.GetContentSha256(),
			"size":             stat.GetSize(),
		}),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"id":               confirmed.GetUid(),
		"source_namespace": req.SourceNamespace,
		"source_name":      req.SourceName,
		"dest_namespace":   req.DestNamespace,
		"dest_name":        dstName,
		"size":             stat.GetSize(),
		"sha256":           srcRec.GetContentSha256(),
	})
}
