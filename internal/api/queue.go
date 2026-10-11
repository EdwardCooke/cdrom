package api

import (
	"net/http"

	"cdrom/internal/authz"
	dbpb "cdrom/internal/gen/cdrom/db/v1"
)

// ---------------------------------------------------------------------------
// Queue (F-16)
// ---------------------------------------------------------------------------

// getQueue returns the pending queue (F-16): the jobs waiting to run (each
// with the resource(s) blocking it, if any), the per-group and global load,
// and the per-key load. The UI uses it to show the backlog and which
// concurrency key(s) are holding a job up. It is a read-only, platform-wide
// view, so it is gated on the workers.can-view permission (the same one that
// gates the worker list).
func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	if !s.requirePermission(w, r, authz.PermWorkersView, authz.Resource{}) {
		return
	}
	response, err := s.clients.Database.GetQueueStatus(r.Context(), &dbpb.GetQueueStatusRequest{})
	if err != nil {
		grpcError(w, err)
		return
	}
	if response == nil {
		response = &dbpb.QueueStatus{}
	}
	writeJSON(w, http.StatusOK, response)
}
