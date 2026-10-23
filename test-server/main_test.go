package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var testStart = time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

// testServer is a server whose clock only moves when the test advances it.
type testServer struct {
	*server
	clock time.Time
}

func newTestServer(cfg config) *testServer {
	ts := &testServer{server: newServer(cfg, testStart), clock: testStart}
	ts.now = func() time.Time { return ts.clock }
	return ts
}

func (ts *testServer) do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	rec := ts.raw(t, method, url, body, "Bearer "+testAccessToken)
	return rec.Code, decodeObject(t, rec)
}

func (ts *testServer) raw(t *testing.T, method, url, body, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	if method == http.MethodPost && strings.Contains(url, "/oidc/") {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	rec := httptest.NewRecorder()
	ts.ServeHTTP(rec, req)
	return rec
}

func decodeObject(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not a JSON object: %v: %s", err, rec.Body.String())
	}
	return out
}

const (
	accountURL = "https://localhost:8765"
	alphaURL   = "https://ws-alpha.localhost:8765"
	betaURL    = "https://ws-beta.localhost:8765"
)

// connectorStatement has the shape of the connector's audit query (pkg/connector/audit_event_feed.go).
const connectorStatement = `
		SELECT event_id, event_time, workspace_id, action_name, service_name, request_params
		FROM system.access.audit
		WHERE event_date >= :start_date
		  AND (event_time > :start_time OR (event_time = :start_time AND event_id > :start_after_event_id))
		  AND event_time <= :lag_cutoff
		  AND service_name IN ('accounts', 'workspace')
		  AND action_name IN (%s)
		ORDER BY event_time ASC, event_id ASC
		LIMIT %d
	`

const connectorActions = `'activateUser', 'add', 'addPermissionAssignment', 'addPrincipalToGroup', 'addPrincipalsToGroup',
	'changeDatabricksSqlAcl', 'changeDatabricksWorkspaceAcl', 'createGroup', 'deactivateUser', 'delete',
	'deletePermissionAssignment', 'removeAccountAdmin', 'removeAdmin', 'removeGroup', 'removePrincipalFromGroup',
	'removePrincipalsFromGroup', 'setAccountAdmin', 'setAdmin', 'updateGroup', 'updatePermissionAssignment', 'updateUser'`

func statementRequest(t *testing.T, warehouseID, waitTimeout string, startTime time.Time, startAfter string, limit int) string {
	t.Helper()
	cutoff := testStart.Add(-4 * time.Hour)
	body := map[string]any{
		"warehouse_id": warehouseID,
		"statement":    fmt.Sprintf(connectorStatement, connectorActions, limit),
		"wait_timeout": waitTimeout,
		"format":       "JSON_ARRAY",
		"disposition":  "INLINE",
		"parameters": []map[string]string{
			{"name": "start_date", "value": startTime.Format(time.DateOnly), "type": "DATE"},
			{"name": "start_time", "value": startTime.Format(time.RFC3339Nano), "type": "TIMESTAMP"},
			{"name": "start_after_event_id", "value": startAfter, "type": "STRING"},
			{"name": "lag_cutoff", "value": cutoff.Format(time.RFC3339Nano), "type": "TIMESTAMP"},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func eventID(n int) string {
	return fmt.Sprintf("e0000001-0000-4000-8000-%012d", n)
}

func eventIDs(t *testing.T, body map[string]any) []string {
	t.Helper()
	result, _ := body["result"].(map[string]any)
	rows, _ := result["data_array"].([]any)
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		row, _ := r.([]any)
		id, _ := row[0].(string)
		ids = append(ids, id)
	}
	return ids
}

func stmtState(body map[string]any) string {
	status, _ := body["status"].(map[string]any)
	s, _ := status["state"].(string)
	return s
}

func TestHealth(t *testing.T) {
	ts := newTestServer(config{})
	for _, url := range []string{accountURL + "/health", alphaURL + "/health"} {
		if rec := ts.raw(t, http.MethodGet, url, "", ""); rec.Code != http.StatusOK {
			t.Errorf("%s: got %d", url, rec.Code)
		}
	}
}

func TestToken(t *testing.T) {
	ts := newTestServer(config{})
	tokenURL := accountURL + "/oidc/accounts/test-account/v1/token"
	form := "grant_type=client_credentials&scope=all-apis"

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tokenURL, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(testClientID, testClientSecret)
	rec := httptest.NewRecorder()
	ts.ServeHTTP(rec, req)
	body := decodeObject(t, rec)
	if rec.Code != http.StatusOK || body["access_token"] != testAccessToken || body["token_type"] != "Bearer" {
		t.Fatalf("good credentials: got %d %v", rec.Code, body)
	}

	req = httptest.NewRequestWithContext(t.Context(), http.MethodPost, tokenURL, strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(testClientID, "wrong")
	rec = httptest.NewRecorder()
	ts.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad secret: got %d", rec.Code)
	}
}

func TestBearerRequired(t *testing.T) {
	ts := newTestServer(config{})
	for _, auth := range []string{"", "Bearer wrong"} {
		rec := ts.raw(t, http.MethodGet, accountURL+"/api/2.0/accounts/test-account/workspaces", "", auth)
		body := decodeObject(t, rec)
		if rec.Code != http.StatusUnauthorized || body["error_code"] != "UNAUTHENTICATED" {
			t.Errorf("auth %q: got %d %v", auth, rec.Code, body)
		}
	}
}

func TestAccountEndpoints(t *testing.T) {
	ts := newTestServer(config{})

	rec := ts.raw(t, http.MethodGet, accountURL+"/api/2.0/accounts/test-account/workspaces", "", "Bearer "+testAccessToken)
	var workspaces []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &workspaces); err != nil || len(workspaces) != 3 {
		t.Fatalf("workspaces: want a bare array of 3, got %d %s", rec.Code, rec.Body.String())
	}
	// Large IDs must come out as exact integers, not floats in scientific notation.
	if !strings.Contains(rec.Body.String(), `"workspace_id":7474645012345671`) {
		t.Errorf("workspace_id not rendered as an exact integer: %s", rec.Body.String())
	}

	if code, _ := ts.do(t, http.MethodGet, accountURL+"/api/2.0/preview/accounts/test-account/access-control/assignable-roles", ""); code != http.StatusOK {
		t.Errorf("assignable roles: got %d", code)
	}
	if code, _ := ts.do(t, http.MethodGet, accountURL+"/api/2.0/accounts/other-account/workspaces", ""); code != http.StatusNotFound {
		t.Errorf("other account: got %d", code)
	}

	scim := accountURL + "/api/2.0/accounts/test-account/scim/v2/"
	cases := []struct {
		path string
		want int
	}{
		{"Users/" + userAlice, http.StatusOK},
		{"Groups/" + groupAnalysts, http.StatusOK},
		{"ServicePrincipals/" + spCIBot, http.StatusOK},
		{"Users/" + groupAnalysts, http.StatusNotFound},
		{"Users/" + deletedPrincipal, http.StatusNotFound},
		{"Groups/" + deletedPrincipal, http.StatusNotFound},
		{"ServicePrincipals/" + deletedPrincipal, http.StatusNotFound},
	}
	for _, c := range cases {
		code, body := ts.do(t, http.MethodGet, scim+c.path, "")
		if code != c.want {
			t.Errorf("%s: got %d, want %d", c.path, code, c.want)
		}
		if c.want == http.StatusNotFound && body["status"] != "404" {
			t.Errorf("%s: want a SCIM error body, got %v", c.path, body)
		}
	}
}

func TestRouting(t *testing.T) {
	ts := newTestServer(config{})
	if code, _ := ts.do(t, http.MethodGet, "https://ws-nope.localhost:8765/api/2.0/sql/warehouses/wh-serverless", ""); code != http.StatusNotFound {
		t.Errorf("unknown workspace host: got %d", code)
	}
	if code, _ := ts.do(t, http.MethodGet, alphaURL+"/api/2.0/sql/warehouses/wh-serverless", ""); code != http.StatusOK {
		t.Errorf("warehouse in its workspace: got %d", code)
	}
	if code, _ := ts.do(t, http.MethodGet, betaURL+"/api/2.0/sql/warehouses/wh-serverless", ""); code != http.StatusNotFound {
		t.Errorf("warehouse from another workspace: got %d", code)
	}
	if code, _ := ts.do(t, http.MethodGet, accountURL+"/api/2.0/sql/warehouses/wh-serverless", ""); code != http.StatusNotFound {
		t.Errorf("workspace API on the account host: got %d", code)
	}
}

func TestWaitTimeoutValidation(t *testing.T) {
	ts := newTestServer(config{})
	for _, wait := range []string{"0s", "5s", "50s"} {
		code, _ := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-serverless", wait, testStart.Add(-48*time.Hour), "", 10))
		if code != http.StatusOK {
			t.Errorf("wait_timeout %s: got %d", wait, code)
		}
	}
	for _, wait := range []string{"1s", "4s", "51s", "10", "5m", "-5s"} {
		code, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-serverless", wait, testStart.Add(-48*time.Hour), "", 10))
		if code != http.StatusBadRequest || body["error_code"] != "INVALID_PARAMETER_VALUE" {
			t.Errorf("wait_timeout %s: got %d %v", wait, code, body)
		}
	}
}

func TestStatementErrors(t *testing.T) {
	ts := newTestServer(config{})
	url := alphaURL + "/api/2.0/sql/statements"

	code, _ := ts.do(t, http.MethodPost, url, statementRequest(t, "wh-missing", "0s", testStart, "", 10))
	if code != http.StatusNotFound {
		t.Errorf("unknown warehouse: got %d", code)
	}
	code, _ = ts.do(t, http.MethodPost, url, `{"warehouse_id":"wh-serverless","statement":"SELECT ?","parameters":[{"value":"1"}]}`)
	if code != http.StatusBadRequest {
		t.Errorf("positional parameter: got %d", code)
	}

	code, body := ts.do(t, http.MethodPost, url, `{"warehouse_id":"wh-serverless","statement":"SELECT 1","wait_timeout":"0s"}`)
	status, _ := body["status"].(map[string]any)
	errBody, _ := status["error"].(map[string]any)
	msg, _ := errBody["message"].(string)
	if code != http.StatusOK || stmtState(body) != statementFailed || !strings.Contains(msg, "test-server: unsupported statement") {
		t.Errorf("unsupported statement: got %d %v", code, body)
	}

	code, body = ts.do(t, http.MethodPost, url,
		`{"warehouse_id":"wh-serverless","statement":"SELECT event_id FROM system.access.audit WHERE event_time > :t ORDER BY event_time, event_id","wait_timeout":"0s"}`)
	if code != http.StatusOK || stmtState(body) != statementFailed {
		t.Errorf("unbound parameter: got %d %v", code, body)
	}

	if code, _ := ts.do(t, http.MethodGet, url+"/no-such-statement", ""); code != http.StatusNotFound {
		t.Errorf("unknown statement: got %d", code)
	}
}

func TestAuditQueryRunningWarehouse(t *testing.T) {
	ts := newTestServer(config{})
	code, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements",
		statementRequest(t, "wh-serverless", "30s", testStart.Add(-48*time.Hour), "", 1000))
	if code != http.StatusOK || stmtState(body) != statementSucceeded {
		t.Fatalf("got %d %v", code, body)
	}

	// Visible from us-east-1, inside (start-48h, start-4h], in the requested service/action
	// lists: every seed row except 18 (eu-west-1), 24 (clusters), 25 (too new) and 26 (too old).
	var want []string
	for n := 1; n <= 23; n++ {
		if n != 18 {
			want = append(want, eventID(n))
		}
	}
	if got := eventIDs(t, body); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows:\n got %v\nwant %v", got, want)
	}

	manifest, _ := body["manifest"].(map[string]any)
	schema, _ := manifest["schema"].(map[string]any)
	columns, _ := schema["columns"].([]any)
	if len(columns) != 6 {
		t.Errorf("want 6 columns, got %v", columns)
	}
	result, _ := body["result"].(map[string]any)
	if _, ok := result["next_chunk_index"]; ok {
		t.Errorf("single chunk must not have next_chunk_index: %v", result)
	}

	rows, _ := result["data_array"].([]any)
	first, _ := rows[0].([]any)
	if first[1] != testStart.Add(-26*time.Hour).Format("2006-01-02T15:04:05.000Z") || first[2] != "0" {
		t.Errorf("unexpected value formats: %v", first)
	}
	var params map[string]string
	if s, _ := first[5].(string); json.Unmarshal([]byte(s), &params) != nil || params["targetUserId"] != userDave {
		t.Errorf("request_params is not a JSON object string: %v", first[5])
	}
}

func TestAuditQueryPaging(t *testing.T) {
	ts := newTestServer(config{})
	rowTime := func(n int) time.Time {
		for _, r := range ts.st.audit {
			if r.EventID == eventID(n) {
				return r.EventTime
			}
		}
		t.Fatalf("no seed row %d", n)
		return time.Time{}
	}

	_, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-serverless", "10s", rowTime(5), eventID(5), 3))
	if got := eventIDs(t, body); strings.Join(got, ",") != strings.Join([]string{eventID(6), eventID(7), eventID(8)}, ",") {
		t.Errorf("page after row 5: got %v", got)
	}

	// Rows 21 and 22 share an event_time: the event_id tiebreaker must return only 22.
	_, body = ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-serverless", "10s", rowTime(21), eventID(21), 1))
	if got := eventIDs(t, body); len(got) != 1 || got[0] != eventID(22) {
		t.Errorf("tiebreak after row 21: got %v", got)
	}
}

func TestRegionality(t *testing.T) {
	ts := newTestServer(config{})
	stmt := fmt.Sprintf(connectorStatement, connectorActions, 1000)
	params := []statementParameter{
		{Name: "start_date", Value: ptr(testStart.Add(-48 * time.Hour).Format(time.DateOnly)), Type: "DATE"},
		{Name: "start_time", Value: ptr(testStart.Add(-48 * time.Hour).Format(time.RFC3339Nano)), Type: "TIMESTAMP"},
		{Name: "start_after_event_id", Value: ptr(""), Type: "STRING"},
		{Name: "lag_cutoff", Value: ptr(testStart.Add(-4 * time.Hour).Format(time.RFC3339Nano)), Type: "TIMESTAMP"},
	}
	_, rows, err := ts.st.runAuditQuery(stmt, params, "eu-west-1")
	if err != nil {
		t.Fatal(err.message)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[*r[0]] = true
	}
	if !got[eventID(18)] {
		t.Error("eu-west-1 should see the ws-gamma row")
	}
	if !got[eventID(1)] {
		t.Error("eu-west-1 should see account-level rows")
	}
	for _, n := range []int{11, 14, 19} {
		if got[eventID(n)] {
			t.Errorf("eu-west-1 should not see us-east-1 workspace row %d", n)
		}
	}
}

func ptr(s string) *string { return &s }

func TestStoppedWarehouseColdStart(t *testing.T) {
	ts := newTestServer(config{warehouseStartDelay: time.Minute})
	_, body := ts.do(t, http.MethodPost, betaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-pro", "0s", testStart.Add(-48*time.Hour), "", 1000))
	if stmtState(body) != statementPending {
		t.Fatalf("want PENDING while the warehouse starts, got %v", body)
	}
	id, _ := body["statement_id"].(string)

	if _, wh := ts.do(t, http.MethodGet, betaURL+"/api/2.0/sql/warehouses/wh-pro", ""); wh["state"] != warehouseStarting {
		t.Errorf("want STARTING, got %v", wh["state"])
	}
	if _, polled := ts.do(t, http.MethodGet, betaURL+"/api/2.0/sql/statements/"+id, ""); stmtState(polled) != statementPending {
		t.Errorf("want PENDING before the start delay, got %v", polled)
	}

	ts.clock = ts.clock.Add(time.Minute)
	if _, wh := ts.do(t, http.MethodGet, betaURL+"/api/2.0/sql/warehouses/wh-pro", ""); wh["state"] != warehouseRunning {
		t.Errorf("want RUNNING, got %v", wh["state"])
	}
	_, polled := ts.do(t, http.MethodGet, betaURL+"/api/2.0/sql/statements/"+id, "")
	if stmtState(polled) != statementSucceeded || len(eventIDs(t, polled)) != 22 {
		t.Errorf("want SUCCEEDED with 22 rows after the start delay, got %v", polled)
	}
}

func TestBrokenWarehouseAndCancel(t *testing.T) {
	ts := newTestServer(config{})
	_, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-broken", "0s", testStart.Add(-48*time.Hour), "", 1000))
	id, _ := body["statement_id"].(string)

	ts.clock = ts.clock.Add(time.Hour)
	if _, polled := ts.do(t, http.MethodGet, alphaURL+"/api/2.0/sql/statements/"+id, ""); stmtState(polled) != statementPending {
		t.Errorf("want PENDING forever, got %v", polled)
	}
	_, wh := ts.do(t, http.MethodGet, alphaURL+"/api/2.0/sql/warehouses/wh-broken", "")
	health, _ := wh["health"].(map[string]any)
	if wh["state"] != warehouseStopped || health["status"] != "FAILED" {
		t.Errorf("want STOPPED with FAILED health, got %v", wh)
	}

	code, cancel := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements/"+id+"/cancel", "")
	if code != http.StatusOK || len(cancel) != 0 {
		t.Errorf("cancel: want 200 {}, got %d %v", code, cancel)
	}
	if _, polled := ts.do(t, http.MethodGet, alphaURL+"/api/2.0/sql/statements/"+id, ""); stmtState(polled) != statementCanceled {
		t.Errorf("want CANCELED after cancel, got %v", polled)
	}
}

func TestOnWaitTimeoutCancel(t *testing.T) {
	ts := newTestServer(config{})
	req := strings.Replace(statementRequest(t, "wh-broken", "5s", testStart.Add(-48*time.Hour), "", 1000),
		`"wait_timeout":"5s"`, `"wait_timeout":"5s","on_wait_timeout":"CANCEL"`, 1)
	if _, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", req); stmtState(body) != statementCanceled {
		t.Errorf("want CANCELED after wait_timeout with on_wait_timeout=CANCEL, got %v", body)
	}
}

func TestChunks(t *testing.T) {
	ts := newTestServer(config{chunkRows: 10})
	_, body := ts.do(t, http.MethodPost, alphaURL+"/api/2.0/sql/statements", statementRequest(t, "wh-serverless", "10s", testStart.Add(-48*time.Hour), "", 1000))
	id, _ := body["statement_id"].(string)
	manifest, _ := body["manifest"].(map[string]any)
	if manifest["total_chunk_count"] != float64(3) || manifest["total_row_count"] != float64(22) {
		t.Fatalf("want 3 chunks / 22 rows, got %v", manifest)
	}

	all := eventIDs(t, body)
	next, _ := body["result"].(map[string]any)["next_chunk_index"].(float64)
	for next > 0 {
		_, chunk := ts.do(t, http.MethodGet, fmt.Sprintf("%s/api/2.0/sql/statements/%s/result/chunks/%d", alphaURL, id, int(next)), "")
		all = append(all, eventIDs(t, map[string]any{"result": chunk})...)
		next, _ = chunk["next_chunk_index"].(float64)
	}
	if len(all) != 22 {
		t.Errorf("want 22 rows across chunks, got %d", len(all))
	}
	if code, _ := ts.do(t, http.MethodGet, fmt.Sprintf("%s/api/2.0/sql/statements/%s/result/chunks/3", alphaURL, id), ""); code != http.StatusBadRequest {
		t.Errorf("out-of-range chunk: got %d", code)
	}
}
