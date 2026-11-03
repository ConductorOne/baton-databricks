package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// DECISION: hardcoded test credentials.
const (
	testAccountID    = "test-account"
	testClientID     = "test-client"
	testClientSecret = "test-secret"
	testAccessToken  = "test-access-token"
)

type workspace struct {
	ID             int64
	Name           string
	DeploymentName string
	Region         string
}

type scimUser struct {
	ID          string
	UserName    string
	DisplayName string
	Active      bool
}

type scimGroup struct {
	ID          string
	DisplayName string
}

type scimServicePrincipal struct {
	ID            string
	ApplicationID string
	DisplayName   string
	Active        bool
}

type warehouse struct {
	ID             string
	Name           string
	DeploymentName string // workspace hosting the warehouse
	WarehouseType  string
	Serverless     bool
	InitialState   string
	// NeverStarts makes start attempts fail: the warehouse stays STOPPED with a FAILED health status.
	NeverStarts bool

	startRequestedAt time.Time
}

type auditRow struct {
	EventID       string
	EventTime     time.Time
	WorkspaceID   int64
	ServiceName   string
	ActionName    string
	RequestParams map[string]string
}

// state is the in-memory account, seeded at startup (see seeds.go) and reset on every start.
type state struct {
	workspaces        []workspace
	users             map[string]scimUser
	groups            map[string]scimGroup
	servicePrincipals map[string]scimServicePrincipal
	warehouses        map[string]*warehouse
	audit             []auditRow
	statements        map[string]*statement
	nextStatement     int
}

func (st *state) workspaceByDeployment(name string) *workspace {
	for i := range st.workspaces {
		if st.workspaces[i].DeploymentName == name {
			return &st.workspaces[i]
		}
	}
	return nil
}

func (st *state) workspaceByID(id int64) *workspace {
	for i := range st.workspaces {
		if st.workspaces[i].ID == id {
			return &st.workspaces[i]
		}
	}
	return nil
}

type server struct {
	cfg   config
	start time.Time
	now   func() time.Time

	mu sync.Mutex
	st *state

	account   *http.ServeMux
	workspace *http.ServeMux
}

func newServer(cfg config, start time.Time) *server {
	s := &server{
		cfg:   cfg,
		start: start,
		now:   time.Now,
		st:    seedState(start),
	}
	s.account = s.accountRoutes()
	s.workspace = s.workspaceRoutes()
	return s
}

type workspaceKey struct{}

func requestWorkspace(r *http.Request) *workspace {
	ws, _ := r.Context().Value(workspaceKey{}).(*workspace)
	return ws
}

// ServeHTTP routes by Host header (DECISION): localhost is the account host, and
// <deployment_name>.localhost is a workspace host, mirroring <deployment_name>.<hostname>.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// DECISION: local readiness check on every host; not a Databricks endpoint.
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	host := r.Host
	if h, _, err := net.SplitHostPort(r.Host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))

	switch {
	case host == "localhost" || host == "127.0.0.1" || host == "::1":
		s.account.ServeHTTP(w, r)
	case strings.HasSuffix(host, ".localhost"):
		deployment := strings.TrimSuffix(host, ".localhost")
		s.mu.Lock()
		ws := s.st.workspaceByDeployment(deployment)
		s.mu.Unlock()
		if ws == nil {
			// DECISION: a real unknown deployment name doesn't resolve in DNS at all.
			writeError(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", "test-server: no workspace with deployment name "+deployment)
			return
		}
		s.workspace.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), workspaceKey{}, ws)))
	default:
		writeError(w, http.StatusNotFound, "ENDPOINT_NOT_FOUND", "test-server: unknown host "+host)
	}
}

// requireBearer enforces the access token on every API call except the token endpoint.
// DOC: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m (Authorization: Bearer <token>).
// DOC: error body {error_code, message}, https://docs.databricks.com/api/workspace/introduction.
func requireBearer(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token != testAccessToken {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Credential was not sent or was of an unsupported type for this API.")
			return
		}
		next(w, r)
	}
}

// requireAccount rejects account API paths for any account other than testAccountID.
// ASSUMPTION: the exact status and code for another account's ID are not documented.
func requireAccount(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("account_id") != testAccountID {
			writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "Account "+r.PathValue("account_id")+" does not exist.")
			return
		}
		next(w, r)
	}
}
