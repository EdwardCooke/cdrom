package database

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	dbpb "cdrom/internal/gen/cdrom/db/v1"
	"cdrom/internal/models"
)

// combineTags merges the given tag sets in order, with later (more specific)
// sets overriding earlier ones on a key collision. The result is a new map; a
// nil or empty result is returned as nil. This is the F-29 combining rule:
// callers pass the sets from least to most specific (e.g. pipeline, job,
// environment, worker group) so the most specific source wins on a collision.
func combineTags(sets ...map[string]string) map[string]string {
	out := make(map[string]string)
	for _, set := range sets {
		for k, v := range set {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// replaceTags replaces the tags of the resource identified by ownerType and
// ownerID with the given set (F-29). It deletes the resource's existing tag
// rows and inserts one row per key/value pair in the new set, so the set is
// replaced wholesale (a key not in the new set is removed). An empty or nil
// set clears the resource's tags. The rows are written to the normalized Tag
// table (one row per pair) rather than a JSON column, so they can be searched
// by key and/or value. A zero ownerID is a no-op (the resource is not yet
// persisted).
func replaceTags(db *gorm.DB, ownerType string, ownerID uint, tags map[string]string) error {
	if ownerID == 0 {
		return nil
	}
	if err := db.Where("owner_type = ? AND owner_id = ?", ownerType, ownerID).Delete(&models.Tag{}).Error; err != nil {
		return err
	}
	if len(tags) == 0 {
		return nil
	}
	rows := make([]models.Tag, 0, len(tags))
	for k, v := range tags {
		rows = append(rows, models.Tag{OwnerType: ownerType, OwnerID: ownerID, Key: k, Value: v})
	}
	return db.Create(&rows).Error
}

// loadTags loads the tags of the resource identified by ownerType and ownerID
// (F-29) into a map (nil when the resource has no tags). A zero ownerID
// yields nil.
func loadTags(db *gorm.DB, ownerType string, ownerID uint) map[string]string {
	if ownerID == 0 {
		return nil
	}
	var rows []models.Tag
	if err := db.Where("owner_type = ? AND owner_id = ?", ownerType, ownerID).Find(&rows).Error; err != nil {
		return nil
	}
	if len(rows) == 0 {
		return nil
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Key] = r.Value
	}
	return out
}

// loadTagsByOwner loads the tags of every resource of the given ownerType
// whose id is in ownerIDs (F-29), keyed by owner id. A resource with no tags
// is absent from the result. This hydrates the in-memory Tags field of a list
// of resources in a single query.
func loadTagsByOwner(db *gorm.DB, ownerType string, ownerIDs []uint) map[uint]map[string]string {
	if len(ownerIDs) == 0 {
		return nil
	}
	var rows []models.Tag
	if err := db.Where("owner_type = ? AND owner_id IN ?", ownerType, ownerIDs).Find(&rows).Error; err != nil {
		return nil
	}
	out := make(map[uint]map[string]string, len(rows))
	for _, r := range rows {
		m := out[r.OwnerID]
		if m == nil {
			m = make(map[string]string)
			out[r.OwnerID] = m
		}
		m[r.Key] = r.Value
	}
	return out
}

// ownerIDsMatchingTags returns the ids of the resources of the given
// ownerType that carry every one of the given key/value tags (F-29): a
// resource matches when it has a tag row for each key with the corresponding
// value. This is what makes a resource searchable by tag — the tags are
// stored normalized in the Tag table (one row per pair), so the match is a
// plain indexed SQL query rather than a scan of a JSON column. An empty or
// nil tag set matches every resource (the caller is expected to skip the
// filter in that case).
func ownerIDsMatchingTags(db *gorm.DB, ownerType string, tags map[string]string) ([]uint, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	// For each key/value pair, the set of owner ids that have that exact tag.
	// A resource matches only if it is in the intersection of every pair's set.
	perPair := make([]map[uint]struct{}, 0, len(tags))
	for k, v := range tags {
		var ids []uint
		if err := db.Model(&models.Tag{}).
			Where("owner_type = ? AND key = ? AND value = ?", ownerType, k, v).
			Distinct().Pluck("owner_id", &ids).Error; err != nil {
			return nil, err
		}
		set := make(map[uint]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		perPair = append(perPair, set)
	}
	// Intersect the per-pair sets: a resource must have every tag.
	intersection := perPair[0]
	for _, set := range perPair[1:] {
		for id := range intersection {
			if _, ok := set[id]; !ok {
				delete(intersection, id)
			}
		}
	}
	out := make([]uint, 0, len(intersection))
	for id := range intersection {
		out = append(out, id)
	}
	return out, nil
}

// runIDsMatchingTags returns the ids of the runs that have at least one job
// instance carrying every one of the given key/value tags (F-29). A run's tags
// are the combined tags stored on its job instances, so a run matches when any
// of its job instances has a tag row for each key with the corresponding
// value. An empty or nil tag set matches every run (the caller is expected to
// skip the filter in that case).
func (s *Server) runIDsMatchingTags(ctx context.Context, tags map[string]string) ([]uint, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	db := s.db.WithContext(ctx)
	// The job instances that carry every one of the given tags.
	jobIDs, err := ownerIDsMatchingTags(db, models.TagOwnerJob, tags)
	if err != nil {
		return nil, err
	}
	if len(jobIDs) == 0 {
		return nil, nil
	}
	// The runs those job instances belong to. run_id is a nullable column, so
	// the jobs are loaded (not Plucked) and their RunID read directly; a job
	// that is not part of a run (run_id nil) is skipped.
	var jobs []models.Job
	if err := db.Where("id IN ?", jobIDs).Find(&jobs).Error; err != nil {
		return nil, err
	}
	seen := make(map[uint]struct{}, len(jobs))
	for i := range jobs {
		if jobs[i].RunID != nil {
			seen[*jobs[i].RunID] = struct{}{}
		}
	}
	out := make([]uint, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out, nil
}

// hydrateJobTags loads the tags of the given jobs (F-29) from the Tag table
// into their in-memory fields, so they can be converted to their proto form. A
// job with no tags is left nil.
func (s *Server) hydrateJobTags(ctx context.Context, jobs []models.Job) {
	if len(jobs) == 0 {
		return
	}
	db := s.db.WithContext(ctx)
	ids := make([]uint, len(jobs))
	for i := range jobs {
		ids[i] = jobs[i].ID
	}
	byOwner := loadTagsByOwner(db, models.TagOwnerJob, ids)
	for i := range jobs {
		jobs[i].Tags = byOwner[jobs[i].ID]
	}
}

// hydrateWorkerTags loads the tags of the given workers (F-29) from the Tag
// table into their in-memory fields.
func (s *Server) hydrateWorkerTags(ctx context.Context, workers []models.Worker) {
	if len(workers) == 0 {
		return
	}
	db := s.db.WithContext(ctx)
	ids := make([]uint, len(workers))
	for i := range workers {
		ids[i] = workers[i].ID
	}
	byOwner := loadTagsByOwner(db, models.TagOwnerWorker, ids)
	for i := range workers {
		workers[i].Tags = byOwner[workers[i].ID]
	}
}

// hydrateEnvironmentTags loads the tags of the given environments (F-29) from
// the Tag table into their in-memory fields.
func (s *Server) hydrateEnvironmentTags(ctx context.Context, envs []models.Environment) {
	if len(envs) == 0 {
		return
	}
	db := s.db.WithContext(ctx)
	ids := make([]uint, len(envs))
	for i := range envs {
		ids[i] = envs[i].ID
	}
	byOwner := loadTagsByOwner(db, models.TagOwnerEnvironment, ids)
	for i := range envs {
		envs[i].Tags = byOwner[envs[i].ID]
	}
}

// hydrateVersionTags loads the tags of the given pipeline version snapshots
// (F-29) from the Tag table into their in-memory fields. The snapshots' job
// tags are captured in the snapshot's JSON document, so only the
// pipeline-level tags (owner_type "pipeline_version") are loaded here.
func (s *Server) hydrateVersionTags(ctx context.Context, versions []models.PipelineVersion) {
	if len(versions) == 0 {
		return
	}
	db := s.db.WithContext(ctx)
	ids := make([]uint, len(versions))
	for i := range versions {
		ids[i] = versions[i].ID
	}
	byOwner := loadTagsByOwner(db, models.TagOwnerPipelineVersion, ids)
	for i := range versions {
		versions[i].Tags = byOwner[versions[i].ID]
	}
}

// hydrateExecutionTags loads the tags of the given job executions (F-29) from
// the Tag table into their in-memory fields.
func (s *Server) hydrateExecutionTags(ctx context.Context, executions []models.JobExecution) {
	if len(executions) == 0 {
		return
	}
	db := s.db.WithContext(ctx)
	ids := make([]uint, len(executions))
	for i := range executions {
		ids[i] = executions[i].ID
	}
	byOwner := loadTagsByOwner(db, models.TagOwnerJobExecution, ids)
	for i := range executions {
		executions[i].Tags = byOwner[executions[i].ID]
	}
}

// hydratePipelineTags loads the pipeline's tags (F-29) from the Tag table into
// its in-memory field, so the pipeline can be converted to its proto form with
// its tags populated. The pipeline's job definitions' tags are hydrated by
// loadPipelineJobDefinitions (the single place they are read from the
// database), so they are already populated when this is called.
func (s *Server) hydratePipelineTags(ctx context.Context, pipeline *models.Pipeline) {
	pipeline.Tags = loadTags(s.db.WithContext(ctx), models.TagOwnerPipeline, pipeline.ID)
}

// runBaseTags computes a job instance's base tag set (F-29): the union of the
// tags of the pipeline, the job, the environment it targets (if any), and the
// worker group it targets (if any), with the more specific source winning on a
// key collision (worker group > environment > job > pipeline). The environment
// and worker-group tags are looked up by name inside the caller's transaction
// (and read from the Tag table); a missing environment or group contributes no
// tags (a job that targets a group that has no WorkerGroup row still runs —
// the row only adds metadata).
func (s *Server) runBaseTags(tx *gorm.DB, pipelineTags, jobTags map[string]string, environment, targetGroup string) map[string]string {
	var envTags, groupTags map[string]string
	if environment != "" {
		var env models.Environment
		if err := tx.Where("name = ?", environment).First(&env).Error; err == nil {
			envTags = loadTags(tx, models.TagOwnerEnvironment, env.ID)
		}
	}
	if targetGroup != "" {
		var group models.WorkerGroup
		if err := tx.Where("name = ?", targetGroup).First(&group).Error; err == nil {
			groupTags = loadTags(tx, models.TagOwnerWorkerGroup, group.ID)
		}
	}
	return combineTags(pipelineTags, jobTags, envTags, groupTags)
}

// ---------------------------------------------------------------------------
// Worker groups (F-29)
// ---------------------------------------------------------------------------
//
// A worker group is a named group of long-lived workers (a job's target_group
// names it; a worker's group names it). It is a first-class, taggable row: its
// tags are combined onto the runs of the jobs that target it. The row is
// metadata-only — it does not change dispatch (which still keys off the
// worker's declared group string) — so a group that has no row simply
// contributes no tags and jobs targeting it still run on its workers.

// CreateWorkerGroup adds a worker group (F-29). It is idempotent on name: a
// create for an existing group is a no-op that returns the existing row (its
// description and tags are left unchanged).
func (s *Server) CreateWorkerGroup(ctx context.Context, req *dbpb.CreateWorkerGroupRequest) (*dbpb.WorkerGroup, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker group name is required")
	}
	db := s.db.WithContext(ctx)
	var group models.WorkerGroup
	err := db.Where("name = ?", req.GetName()).First(&group).Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, grpcErr(err)
		}
		// A new group: create it with the supplied description, then write its
		// tags (F-29) to the Tag table (owner_type "worker_group").
		group = models.WorkerGroup{Name: req.GetName(), Description: req.GetDescription()}
		if err := db.Create(&group).Error; err != nil {
			return nil, grpcErr(err)
		}
		if err := replaceTags(db, models.TagOwnerWorkerGroup, group.ID, req.GetTags()); err != nil {
			return nil, grpcErr(err)
		}
		group.Tags = req.GetTags()
		return toProtoWorkerGroup(&group), nil
	}
	// An existing group: a create is a no-op (the existing row is returned
	// unchanged). Its tags are loaded from the Tag table.
	group.Tags = loadTags(db, models.TagOwnerWorkerGroup, group.ID)
	return toProtoWorkerGroup(&group), nil
}

// GetWorkerGroup returns the named worker group. A missing group returns
// NotFound.
func (s *Server) GetWorkerGroup(ctx context.Context, req *dbpb.GetWorkerGroupRequest) (*dbpb.WorkerGroup, error) {
	db := s.db.WithContext(ctx)
	var group models.WorkerGroup
	if err := db.Where("name = ?", req.GetName()).First(&group).Error; err != nil {
		return nil, grpcErr(err)
	}
	group.Tags = loadTags(db, models.TagOwnerWorkerGroup, group.ID)
	return toProtoWorkerGroup(&group), nil
}

// ListWorkerGroups returns every worker group, optionally filtered by name
// prefix.
func (s *Server) ListWorkerGroups(ctx context.Context, req *dbpb.ListWorkerGroupsRequest) (*dbpb.ListWorkerGroupsResponse, error) {
	query := s.db.WithContext(ctx).Model(&models.WorkerGroup{})
	if req.GetNamePrefix() != "" {
		query = query.Where("name LIKE ?", req.GetNamePrefix()+"%")
	}
	var groups []models.WorkerGroup
	if err := query.Order("name").Find(&groups).Error; err != nil {
		return nil, grpcErr(err)
	}
	// Hydrate each group's tags (F-29) from the Tag table in a single query.
	ids := make([]uint, len(groups))
	for i := range groups {
		ids[i] = groups[i].ID
	}
	byOwner := loadTagsByOwner(query, models.TagOwnerWorkerGroup, ids)
	response := &dbpb.ListWorkerGroupsResponse{}
	for i := range groups {
		groups[i].Tags = byOwner[groups[i].ID]
		response.Groups = append(response.Groups, toProtoWorkerGroup(&groups[i]))
	}
	return response, nil
}

// UpdateWorkerGroup edits a worker group's description and/or tags. A missing
// group returns NotFound.
func (s *Server) UpdateWorkerGroup(ctx context.Context, req *dbpb.UpdateWorkerGroupRequest) (*dbpb.WorkerGroup, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker group name is required")
	}
	db := s.db.WithContext(ctx)
	var group models.WorkerGroup
	if err := db.Where("name = ?", req.GetName()).First(&group).Error; err != nil {
		return nil, grpcErr(err)
	}
	group.Description = req.GetDescription()
	// When tags are supplied (F-29) they replace the group's existing tags in
	// the Tag table; when none are supplied the group's tags are left
	// unchanged.
	if req.GetTags() != nil {
		if err := replaceTags(db, models.TagOwnerWorkerGroup, group.ID, req.GetTags()); err != nil {
			return nil, grpcErr(err)
		}
		group.Tags = req.GetTags()
	} else {
		group.Tags = loadTags(db, models.TagOwnerWorkerGroup, group.ID)
	}
	if err := db.Save(&group).Error; err != nil {
		return nil, grpcErr(err)
	}
	return toProtoWorkerGroup(&group), nil
}

// DeleteWorkerGroup removes a worker group. A missing group returns NotFound.
// Deleting a group does not affect the workers that belong to it (they keep
// their group string) or the jobs that target it (they still dispatch to the
// group's workers); it only removes the group's tags.
func (s *Server) DeleteWorkerGroup(ctx context.Context, req *dbpb.DeleteWorkerGroupRequest) (*emptypb.Empty, error) {
	db := s.db.WithContext(ctx)
	var group models.WorkerGroup
	if err := db.Where("name = ?", req.GetName()).First(&group).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "worker group not found")
		}
		return nil, grpcErr(err)
	}
	// Remove the group's tags (F-29) along with the group row.
	if err := db.Where("owner_type = ? AND owner_id = ?", models.TagOwnerWorkerGroup, group.ID).Delete(&models.Tag{}).Error; err != nil {
		return nil, grpcErr(err)
	}
	if err := db.Delete(&group).Error; err != nil {
		return nil, grpcErr(err)
	}
	return &emptypb.Empty{}, nil
}

// toProtoWorkerGroup converts a worker group model into its proto form (F-29).
func toProtoWorkerGroup(group *models.WorkerGroup) *dbpb.WorkerGroup {
	return &dbpb.WorkerGroup{
		Id:          int64(group.ID),
		Name:        group.Name,
		Description: group.Description,
		Tags:        group.Tags,
		CreatedAt:   timestamppb.New(group.CreatedAt),
		UpdatedAt:   timestamppb.New(group.UpdatedAt),
	}
}

// workerTags returns the named worker's tags (F-29), or nil when the worker is
// missing or has no tags. It is used at dispatch (StartJobExecution) to merge
// the assigned worker's tags into the job's execution's tag set.
func (s *Server) workerTags(ctx context.Context, workerName string) map[string]string {
	db := s.db.WithContext(ctx)
	var worker models.Worker
	if err := db.Where("name = ?", workerName).First(&worker).Error; err != nil {
		return nil
	}
	return loadTags(db, models.TagOwnerWorker, worker.ID)
}
