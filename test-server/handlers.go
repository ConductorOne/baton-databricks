package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"
)

func (s *server) accountRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	// DOC: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m
	mux.HandleFunc("POST /oidc/accounts/{account_id}/v1/token", requireAccount(s.handleToken))
	// DOC: https://docs.databricks.com/api/account/accountaccesscontrol/getassignablerolesforresource
	mux.HandleFunc("GET /api/2.0/preview/accounts/{account_id}/access-control/assignable-roles",
		requireBearer(requireAccount(s.handleAssignableRoles)))
	// DOC: https://docs.databricks.com/api/account/workspaces/list
	mux.HandleFunc("GET /api/2.0/accounts/{account_id}/workspaces", requireBearer(requireAccount(s.handleListWorkspaces)))
	// DOC: https://docs.databricks.com/api/account/accountusers/get
	mux.HandleFunc("GET /api/2.0/accounts/{account_id}/scim/v2/Users/{id}", requireBearer(requireAccount(s.handleGetUser)))
	// DOC: https://docs.databricks.com/api/account/accountgroups/get
	mux.HandleFunc("GET /api/2.0/accounts/{account_id}/scim/v2/Groups/{id}", requireBearer(requireAccount(s.handleGetGroup)))
	// DOC: https://docs.databricks.com/api/account/accountserviceprincipals/get
	mux.HandleFunc("GET /api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{id}",
		requireBearer(requireAccount(s.handleGetServicePrincipal)))
	mux.HandleFunc("/", handleNotFound)
	return mux
}

func (s *server) workspaceRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	// DOC: https://docs.databricks.com/api/workspace/warehouses/get
	mux.HandleFunc("GET /api/2.0/sql/warehouses/{id}", requireBearer(s.handleGetWarehouse))
	// DOC: https://docs.databricks.com/api/workspace/statementexecution/executestatement
	mux.HandleFunc("POST /api/2.0/sql/statements", requireBearer(s.handleExecuteStatement))
	// DOC: https://docs.databricks.com/api/workspace/statementexecution/getstatement
	mux.HandleFunc("GET /api/2.0/sql/statements/{id}", requireBearer(s.handleGetStatement))
	// DOC: https://docs.databricks.com/api/workspace/statementexecution/cancelexecution
	mux.HandleFunc("POST /api/2.0/sql/statements/{id}/cancel", requireBearer(s.handleCancelStatement))
	// DOC: https://docs.databricks.com/api/workspace/statementexecution/getstatementresultchunkn
	mux.HandleFunc("GET /api/2.0/sql/statements/{id}/result/chunks/{n}", requireBearer(s.handleGetResultChunk))
	mux.HandleFunc("/", handleNotFound)
	return mux
}

// handleNotFound answers any route this server doesn't implement.
// ASSUMPTION: Databricks answers unknown API paths with 404 ENDPOINT_NOT_FOUND.
func handleNotFound(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", "test-server: no route for "+r.Method+" "+r.URL.Path)
}

// handleToken implements the OAuth M2M client-credentials token endpoint.
// DOC: HTTP Basic client ID/secret, grant_type=client_credentials, scope=all-apis; returns
// {access_token, token_type: "Bearer", expires_in: 3600}.
// ASSUMPTION: failures use the RFC 6749 error body ({error, error_description}) with 400/401.
func (s *server) handleToken(w http.ResponseWriter, r *http.Request) {
	clientID, clientSecret, ok := r.BasicAuth()
	if !ok || clientID != testClientID || clientSecret != testClientSecret {
		writeJSON(w, http.StatusUnauthorized, map[string]string{
			"error":             "invalid_client",
			"error_description": "Client authentication failed",
		})
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": err.Error()})
		return
	}
	if r.PostForm.Get("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "grant_type must be client_credentials",
		})
		return
	}
	if r.PostForm.Get("scope") != "all-apis" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_scope", "error_description": "scope must be all-apis"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token": testAccessToken,
		"token_type":   "Bearer",
		"expires_in":   3600,
		"scope":        "all-apis",
	})
}

// handleAssignableRoles returns the roles assignable on a resource.
// DOC: response shape {roles: [{name}]}. DECISION: a fixed list, whatever the resource.
func (s *server) handleAssignableRoles(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"roles": []map[string]string{
			{"name": "roles/marketplace.admin"},
		},
	})
}

// handleListWorkspaces returns the account's workspaces.
// DOC: a bare JSON array of workspace objects.
func (s *server) handleListWorkspaces(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]map[string]any, 0, len(s.st.workspaces))
	for _, ws := range s.st.workspaces {
		out = append(out, map[string]any{
			"account_id":       testAccountID,
			"workspace_id":     ws.ID,
			"workspace_name":   ws.Name,
			"deployment_name":  ws.DeploymentName,
			"aws_region":       ws.Region,
			"workspace_status": "RUNNING",
			"pricing_tier":     "PREMIUM",
			"creation_time":    s.start.Add(-90 * 24 * time.Hour).UnixMilli(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// DOC: SCIM user, group and service principal objects, per the account SCIM API pages.
func (s *server) handleGetUser(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	u, ok := s.st.users[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		writeSCIMNotFound(w, "User "+r.PathValue("id")+" not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemas":     []string{"urn:ietf:params:scim:schemas:core:2.0:User"},
		"id":          u.ID,
		"userName":    u.UserName,
		"displayName": u.DisplayName,
		"active":      u.Active,
		"emails":      []map[string]any{{"value": u.UserName, "type": "work", "primary": true}},
	})
}

func (s *server) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	g, ok := s.st.groups[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		writeSCIMNotFound(w, "Group "+r.PathValue("id")+" not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemas":     []string{"urn:ietf:params:scim:schemas:core:2.0:Group"},
		"id":          g.ID,
		"displayName": g.DisplayName,
	})
}

func (s *server) handleGetServicePrincipal(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	sp, ok := s.st.servicePrincipals[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		writeSCIMNotFound(w, "ServicePrincipal "+r.PathValue("id")+" not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schemas":       []string{"urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"},
		"id":            sp.ID,
		"applicationId": sp.ApplicationID,
		"displayName":   sp.DisplayName,
		"active":        sp.Active,
	})
}

// writeSCIMNotFound writes a SCIM 404.
// ASSUMPTION: the SCIM error schema (schemas, detail, status); the 404 body is not documented.
func writeSCIMNotFound(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusNotFound, map[string]any{
		"schemas": []string{"urn:ietf:params:scim:api:messages:2.0:Error"},
		"detail":  detail,
		"status":  "404",
	})
}

// handleGetWarehouse returns a warehouse in the request's workspace.
// DOC: warehouse fields and state values (STARTING, RUNNING, STOPPING, STOPPED, DELETING, DELETED).
// ASSUMPTION: a warehouse that isn't in this workspace is 404 RESOURCE_DOES_NOT_EXIST.
func (s *server) handleGetWarehouse(w http.ResponseWriter, r *http.Request) {
	ws := requestWorkspace(r)

	s.mu.Lock()
	defer s.mu.Unlock()

	wh, ok := s.st.warehouses[r.PathValue("id")]
	if !ok || wh.DeploymentName != ws.DeploymentName {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "Warehouse "+r.PathValue("id")+" does not exist.")
		return
	}

	state := s.warehouseState(wh)
	health := map[string]any{"status": "HEALTHY"}
	if wh.NeverStarts {
		// DECISION: the broken warehouse reports why it can't start.
		health = map[string]any{
			"status":  "FAILED",
			"summary": "test-server: this warehouse never starts",
			"failure_reason": map[string]any{
				"code": "INSTANCE_UNREACHABLE",
				"type": "CLOUD_FAILURE",
			},
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":                        wh.ID,
		"name":                      wh.Name,
		"warehouse_type":            wh.WarehouseType,
		"enable_serverless_compute": wh.Serverless,
		"cluster_size":              "2X-Small",
		"min_num_clusters":          1,
		"max_num_clusters":          1,
		"auto_stop_mins":            10,
		"state":                     state,
		"health":                    health,
		"num_clusters":              boolToInt(state == warehouseRunning),
		"creator_name":              "admin@example.com",
		"jdbc_url":                  "jdbc:spark://" + ws.DeploymentName + ".localhost:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/" + wh.ID,
		"odbc_params": map[string]any{
			"hostname": ws.DeploymentName + ".localhost",
			"path":     "/sql/1.0/warehouses/" + wh.ID,
			"protocol": "https",
			"port":     443,
		},
	})
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// apiError is the documented error body: https://docs.databricks.com/api/workspace/introduction.
type apiError struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, apiError{ErrorCode: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("test-server: writing response: %v", err)
	}
}

func parseChunkIndex(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	return n, err == nil && n >= 0
}
