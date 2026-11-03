package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Warehouse states. DOC: https://docs.databricks.com/api/workspace/warehouses/get
const (
	warehouseStarting = "STARTING"
	warehouseRunning  = "RUNNING"
	warehouseStopped  = "STOPPED"
)

// Statement states. DOC: https://docs.databricks.com/api/workspace/statementexecution/getstatement
const (
	statementPending   = "PENDING"
	statementSucceeded = "SUCCEEDED"
	statementFailed    = "FAILED"
	statementCanceled  = "CANCELED"
)

// warehouseState returns wh's state now. Callers hold s.mu.
// ASSUMPTION: submitting a statement starts a stopped warehouse, which is RUNNING after
// --warehouse-start-delay. DECISION: a NeverStarts warehouse stays STOPPED.
func (s *server) warehouseState(wh *warehouse) string {
	if wh.InitialState != warehouseStopped {
		return wh.InitialState
	}
	if wh.startRequestedAt.IsZero() || wh.NeverStarts {
		return warehouseStopped
	}
	if s.now().Sub(wh.startRequestedAt) >= s.cfg.warehouseStartDelay {
		return warehouseRunning
	}
	return warehouseStarting
}

type column struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	TypeText string `json:"type_text"`
	Position int    `json:"position"`
}

type statement struct {
	ID             string
	DeploymentName string
	WarehouseID    string
	State          string
	ErrorCode      string
	ErrorMessage   string

	// The outcome, decided at submit time and revealed once the warehouse is running.
	outcomeState   string
	outcomeCode    string
	outcomeMessage string
	columns        []column
	rows           [][]*string
}

// refresh moves a PENDING statement to its outcome once its warehouse is running.
// DOC: a statement on a warehouse that isn't running stays PENDING, waiting for the warehouse.
// DECISION: statements go straight from PENDING to SUCCEEDED/FAILED (no visible RUNNING).
func (s *server) refresh(stmt *statement) {
	if stmt.State != statementPending {
		return
	}
	if s.warehouseState(s.st.warehouses[stmt.WarehouseID]) != warehouseRunning {
		return
	}
	stmt.State = stmt.outcomeState
	stmt.ErrorCode = stmt.outcomeCode
	stmt.ErrorMessage = stmt.outcomeMessage
}

func isTerminal(state string) bool {
	return state != statementPending
}

type statementParameter struct {
	Name  string  `json:"name"`
	Value *string `json:"value"`
	Type  string  `json:"type"`
}

type executeRequest struct {
	WarehouseID   string               `json:"warehouse_id"`
	Statement     string               `json:"statement"`
	WaitTimeout   *string              `json:"wait_timeout"`
	OnWaitTimeout string               `json:"on_wait_timeout"`
	Format        string               `json:"format"`
	Disposition   string               `json:"disposition"`
	Parameters    []statementParameter `json:"parameters"`
}

var waitTimeoutRe = regexp.MustCompile(`^(\d+)s$`)

// parseWaitTimeout validates wait_timeout.
// DOC: 0s, or between 5s and 50s; the default is 10s.
func parseWaitTimeout(v *string) (time.Duration, bool) {
	if v == nil {
		return 10 * time.Second, true
	}
	m := waitTimeoutRe.FindStringSubmatch(*v)
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || (n != 0 && (n < 5 || n > 50)) {
		return 0, false
	}
	return time.Duration(n) * time.Second, true
}

// handleExecuteStatement submits a statement.
// DOC: if it finishes within wait_timeout the response has the result; otherwise it has the
// statement ID with PENDING or RUNNING, and on_wait_timeout=CANCEL cancels it instead.
func (s *server) handleExecuteStatement(w http.ResponseWriter, r *http.Request) {
	ws := requestWorkspace(r)

	var req executeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "MALFORMED_REQUEST", "Could not parse request body: "+err.Error())
		return
	}

	wait, ok := parseWaitTimeout(req.WaitTimeout)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE",
			"wait_timeout must be 0 seconds (disables wait), or between 5 seconds and 50 seconds.")
		return
	}
	switch req.OnWaitTimeout {
	case "", "CONTINUE", "CANCEL":
	default:
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "on_wait_timeout must be CONTINUE or CANCEL.")
		return
	}
	// DECISION: only the JSON_ARRAY format and INLINE disposition are implemented.
	if req.Format != "" && req.Format != "JSON_ARRAY" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "test-server: only format JSON_ARRAY is supported")
		return
	}
	if req.Disposition != "" && req.Disposition != "INLINE" {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "test-server: only disposition INLINE is supported")
		return
	}
	// DOC: parameters are named (:name) markers. ASSUMPTION: the status and code for a
	// positional (unnamed) parameter.
	for _, p := range req.Parameters {
		if p.Name == "" {
			writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "Positional parameters are not supported; use named parameter markers.")
			return
		}
	}

	s.mu.Lock()
	wh, ok := s.st.warehouses[req.WarehouseID]
	if !ok || wh.DeploymentName != ws.DeploymentName {
		s.mu.Unlock()
		// DOC: an unknown warehouse_id is an error. ASSUMPTION: its status and code.
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "Warehouse "+req.WarehouseID+" does not exist.")
		return
	}
	if wh.InitialState == warehouseStopped && wh.startRequestedAt.IsZero() {
		wh.startRequestedAt = s.now()
	}

	s.st.nextStatement++
	stmt := &statement{
		ID:             fmt.Sprintf("01f00000-0000-4000-8000-%012d", s.st.nextStatement),
		DeploymentName: ws.DeploymentName,
		WarehouseID:    wh.ID,
		State:          statementPending,
	}
	s.st.statements[stmt.ID] = stmt

	region := ""
	if hostWS := s.st.workspaceByDeployment(wh.DeploymentName); hostWS != nil {
		region = hostWS.Region
	}
	cols, rows, err := s.st.runAuditQuery(req.Statement, req.Parameters, region)
	if err != nil {
		stmt.outcomeState = statementFailed
		stmt.outcomeCode = err.code
		stmt.outcomeMessage = err.message
	} else {
		stmt.outcomeState = statementSucceeded
		stmt.columns = cols
		stmt.rows = rows
	}
	s.refresh(stmt)
	s.mu.Unlock()

	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		s.mu.Lock()
		s.refresh(stmt)
		if isTerminal(stmt.State) {
			body := s.statementBody(stmt)
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, body)
			return
		}
		s.mu.Unlock()

		select {
		case <-r.Context().Done():
			return
		case <-deadline.C:
			s.mu.Lock()
			if req.OnWaitTimeout == "CANCEL" {
				stmt.State = statementCanceled
			}
			body := s.statementBody(stmt)
			s.mu.Unlock()
			writeJSON(w, http.StatusOK, body)
			return
		case <-tick.C:
		}
	}
}

// lookupStatement finds a statement submitted in the request's workspace. Callers hold s.mu.
// ASSUMPTION: an unknown statement ID is 404 RESOURCE_DOES_NOT_EXIST.
func (s *server) lookupStatement(w http.ResponseWriter, r *http.Request) *statement {
	stmt, ok := s.st.statements[r.PathValue("id")]
	if !ok || stmt.DeploymentName != requestWorkspace(r).DeploymentName {
		writeError(w, http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", "Statement "+r.PathValue("id")+" does not exist.")
		return nil
	}
	return stmt
}

// handleGetStatement returns a statement's status.
// DOC: once SUCCEEDED it includes the manifest and the first chunk; terminal states are HTTP 200.
func (s *server) handleGetStatement(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt := s.lookupStatement(w, r)
	if stmt == nil {
		return
	}
	s.refresh(stmt)
	writeJSON(w, http.StatusOK, s.statementBody(stmt))
}

// handleCancelStatement requests cancellation.
// DOC: returns {}; the caller polls the statement to see CANCELED.
func (s *server) handleCancelStatement(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt := s.lookupStatement(w, r)
	if stmt == nil {
		return
	}
	s.refresh(stmt)
	if !isTerminal(stmt.State) {
		stmt.State = statementCanceled
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// handleGetResultChunk returns one result chunk of a SUCCEEDED statement.
// DOC: same shape as the result object, with next_chunk_index absent on the last chunk.
// ASSUMPTION: the status and code for an out-of-range chunk or a statement without a result.
func (s *server) handleGetResultChunk(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stmt := s.lookupStatement(w, r)
	if stmt == nil {
		return
	}
	s.refresh(stmt)
	if stmt.State != statementSucceeded {
		writeError(w, http.StatusBadRequest, "INVALID_STATE", "Statement "+stmt.ID+" has no result: state is "+stmt.State+".")
		return
	}
	n, ok := parseChunkIndex(r.PathValue("n"))
	if !ok || n >= s.chunkCount(stmt) {
		writeError(w, http.StatusBadRequest, "INVALID_PARAMETER_VALUE", "Chunk index "+r.PathValue("n")+" is out of range.")
		return
	}
	writeJSON(w, http.StatusOK, s.chunkBody(stmt, n))
}

// chunkCount is how many chunks stmt's result has. DECISION: one chunk unless --chunk-rows is set.
// ASSUMPTION: an empty result is reported as a single empty chunk.
func (s *server) chunkCount(stmt *statement) int {
	if s.cfg.chunkRows <= 0 || len(stmt.rows) == 0 {
		return 1
	}
	return (len(stmt.rows) + s.cfg.chunkRows - 1) / s.cfg.chunkRows
}

func (s *server) chunkBounds(stmt *statement, n int) (int, int) {
	if s.cfg.chunkRows <= 0 {
		return 0, len(stmt.rows)
	}
	start := n * s.cfg.chunkRows
	return start, min(start+s.cfg.chunkRows, len(stmt.rows))
}

// chunkBody renders result chunk n. DOC: data_array values are strings and nulls are JSON null.
func (s *server) chunkBody(stmt *statement, n int) map[string]any {
	start, end := s.chunkBounds(stmt, n)
	body := map[string]any{
		"chunk_index": n,
		"row_offset":  start,
		"row_count":   end - start,
	}
	if end > start {
		body["data_array"] = stmt.rows[start:end]
	}
	if n+1 < s.chunkCount(stmt) {
		body["next_chunk_index"] = n + 1
		body["next_chunk_internal_link"] = fmt.Sprintf("/api/2.0/sql/statements/%s/result/chunks/%d", stmt.ID, n+1)
	}
	return body
}

// statementBody renders a statement as the Execute/Get responses do. Callers hold s.mu.
func (s *server) statementBody(stmt *statement) map[string]any {
	status := map[string]any{"state": stmt.State}
	if stmt.ErrorMessage != "" {
		status["error"] = map[string]string{"error_code": stmt.ErrorCode, "message": stmt.ErrorMessage}
	}
	body := map[string]any{"statement_id": stmt.ID, "status": status}
	if stmt.State != statementSucceeded {
		return body
	}

	count := s.chunkCount(stmt)
	chunks := make([]map[string]any, 0, count)
	for n := range count {
		start, end := s.chunkBounds(stmt, n)
		chunks = append(chunks, map[string]any{"chunk_index": n, "row_offset": start, "row_count": end - start})
	}
	body["manifest"] = map[string]any{
		"format": "JSON_ARRAY",
		"schema": map[string]any{
			"column_count": len(stmt.columns),
			"columns":      stmt.columns,
		},
		"total_chunk_count": count,
		"chunks":            chunks,
		"total_row_count":   len(stmt.rows),
		"truncated":         false,
	}
	body["result"] = s.chunkBody(stmt, 0)
	return body
}

// SQL handling (DECISION): the server doesn't run SQL. It recognizes the shape of the
// connector's query against system.access.audit and evaluates just that.
var (
	auditFromRe   = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+system\.access\.audit\b`)
	paramMarkerRe = regexp.MustCompile(`(?:^|[^:\w]):([A-Za-z_]\w*)`)
	eventDateRe   = regexp.MustCompile(`(?i)\bevent_date\s*>=\s*:(\w+)`)
	afterRe       = regexp.MustCompile(
		`(?i)\(\s*event_time\s*>\s*:(\w+)\s+OR\s*\(\s*event_time\s*=\s*:(\w+)\s+AND\s+event_id\s*>\s*:(\w+)\s*\)\s*\)`)
	cutoffRe      = regexp.MustCompile(`(?i)\bevent_time\s*<=\s*:(\w+)`)
	serviceListRe = regexp.MustCompile(`(?i)\bservice_name\s+IN\s*\(([^)]*)\)`)
	actionListRe  = regexp.MustCompile(`(?i)\baction_name\s+IN\s*\(([^)]*)\)`)
	orderByRe     = regexp.MustCompile(`(?i)\bORDER\s+BY\s+event_time(\s+ASC)?\s*,\s*event_id(\s+ASC)?\b`)
	limitRe       = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)\s*;?\s*$`)
)

// auditColumns are the documented system.access.audit columns this server serves.
// DOC: https://docs.databricks.com/aws/en/admin/system-tables/audit-logs
// DOC: type_name values, https://docs.databricks.com/api/workspace/statementexecution/executestatement
var auditColumns = map[string]column{
	"event_id":       {Name: "event_id", TypeName: "STRING", TypeText: "STRING"},
	"event_time":     {Name: "event_time", TypeName: "TIMESTAMP", TypeText: "TIMESTAMP"},
	"event_date":     {Name: "event_date", TypeName: "DATE", TypeText: "DATE"},
	"workspace_id":   {Name: "workspace_id", TypeName: "LONG", TypeText: "BIGINT"},
	"service_name":   {Name: "service_name", TypeName: "STRING", TypeText: "STRING"},
	"action_name":    {Name: "action_name", TypeName: "STRING", TypeText: "STRING"},
	"request_params": {Name: "request_params", TypeName: "MAP", TypeText: "MAP<STRING, STRING>"},
}

type sqlError struct {
	code    string
	message string
}

func unsupported(detail string) *sqlError {
	return &sqlError{code: "BAD_REQUEST", message: "test-server: unsupported statement: " + detail}
}

type boundParam struct {
	typ   string
	value string
	time  time.Time
}

// bindParameters binds typed named parameters.
// DOC: named markers with an optional type; the default type is STRING.
// ASSUMPTION: the accepted TIMESTAMP and DATE literal formats.
func bindParameters(params []statementParameter) (map[string]boundParam, *sqlError) {
	bound := make(map[string]boundParam, len(params))
	for _, p := range params {
		typ := strings.ToUpper(p.Type)
		if typ == "" {
			typ = "STRING"
		}
		if p.Value == nil {
			return nil, &sqlError{code: "BAD_REQUEST", message: "test-server: NULL parameter values are not supported"}
		}
		bp := boundParam{typ: typ, value: *p.Value}
		switch typ {
		case "STRING":
		case "TIMESTAMP":
			t, err := time.Parse(time.RFC3339Nano, bp.value)
			if err != nil {
				t, err = time.Parse("2006-01-02 15:04:05.999999", bp.value)
			}
			if err != nil {
				return nil, &sqlError{code: "INVALID_PARAMETER_VALUE", message: fmt.Sprintf("Cannot parse %q as TIMESTAMP for parameter %s.", bp.value, p.Name)}
			}
			bp.time = t.UTC()
		case "DATE":
			t, err := time.Parse(time.DateOnly, bp.value)
			if err != nil {
				return nil, &sqlError{code: "INVALID_PARAMETER_VALUE", message: fmt.Sprintf("Cannot parse %q as DATE for parameter %s.", bp.value, p.Name)}
			}
			bp.time = t
		default:
			return nil, &sqlError{code: "BAD_REQUEST", message: "test-server: unsupported parameter type " + typ}
		}
		bound[p.Name] = bp
	}
	return bound, nil
}

func parseInList(list string) []string {
	var values []string
	for _, v := range strings.Split(list, ",") {
		values = append(values, strings.Trim(strings.TrimSpace(v), "'"))
	}
	return values
}

func contains(values []string, v string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

// timeParam returns a bound TIMESTAMP/DATE parameter. Callers check names against bound first.
func timeParam(bound map[string]boundParam, name string, want string) (time.Time, *sqlError) {
	p := bound[name]
	if p.typ != want {
		return time.Time{}, unsupported(fmt.Sprintf("parameter %s must be bound with type %s", name, want))
	}
	return p.time, nil
}

// runAuditQuery evaluates the connector's audit query against the seed rows visible from a
// warehouse in region. Callers hold s.mu.
// DOC: workspace-level events are only visible from the workspace's region ("Most audit logs
// are only available in the region of the workspace"); account-level events (workspace_id 0)
// are visible from every region.
func (st *state) runAuditQuery(sql string, params []statementParameter, region string) ([]column, [][]*string, *sqlError) {
	m := auditFromRe.FindStringSubmatch(sql)
	if m == nil {
		return nil, nil, unsupported("only SELECT ... FROM system.access.audit is supported")
	}

	var cols []column
	for i, name := range strings.Split(m[1], ",") {
		col, ok := auditColumns[strings.ToLower(strings.TrimSpace(name))]
		if !ok {
			return nil, nil, unsupported(fmt.Sprintf("column %q", strings.TrimSpace(name)))
		}
		col.Position = i
		cols = append(cols, col)
	}

	bound, bindErr := bindParameters(params)
	if bindErr != nil {
		return nil, nil, bindErr
	}
	// ASSUMPTION: the error for a marker with no bound parameter (UNBOUND_SQL_PARAMETER).
	for _, mm := range paramMarkerRe.FindAllStringSubmatch(sql, -1) {
		if _, ok := bound[mm[1]]; !ok {
			return nil, nil, &sqlError{code: "BAD_REQUEST", message: fmt.Sprintf("[UNBOUND_SQL_PARAMETER] Found the unbound parameter: %s.", mm[1])}
		}
	}

	if !orderByRe.MatchString(sql) {
		return nil, nil, unsupported("ORDER BY event_time ASC, event_id ASC is required")
	}

	var (
		startDate, startTime, cutoff time.Time
		startAfterID                 string
		hasStartDate, hasAfter       bool
		hasCutoff                    bool
		services, actions            []string
		limit                        = -1
		err                          *sqlError
	)
	if mm := eventDateRe.FindStringSubmatch(sql); mm != nil {
		if startDate, err = timeParam(bound, mm[1], "DATE"); err != nil {
			return nil, nil, err
		}
		hasStartDate = true
	}
	if mm := afterRe.FindStringSubmatch(sql); mm != nil {
		if mm[1] != mm[2] {
			return nil, nil, unsupported("the two event_time comparisons must use the same parameter")
		}
		if startTime, err = timeParam(bound, mm[1], "TIMESTAMP"); err != nil {
			return nil, nil, err
		}
		startAfterID = bound[mm[3]].value
		hasAfter = true
	}
	if mm := cutoffRe.FindStringSubmatch(sql); mm != nil {
		if cutoff, err = timeParam(bound, mm[1], "TIMESTAMP"); err != nil {
			return nil, nil, err
		}
		hasCutoff = true
	}
	if mm := serviceListRe.FindStringSubmatch(sql); mm != nil {
		services = parseInList(mm[1])
	}
	if mm := actionListRe.FindStringSubmatch(sql); mm != nil {
		actions = parseInList(mm[1])
	}
	if mm := limitRe.FindStringSubmatch(sql); mm != nil {
		limit, _ = strconv.Atoi(mm[1])
	}

	matched := make([]auditRow, 0, len(st.audit))
	for _, row := range st.audit {
		if row.WorkspaceID != 0 {
			ws := st.workspaceByID(row.WorkspaceID)
			if ws == nil || ws.Region != region {
				continue
			}
		}
		eventTime := row.EventTime.UTC()
		eventDate := time.Date(eventTime.Year(), eventTime.Month(), eventTime.Day(), 0, 0, 0, 0, time.UTC)
		if hasStartDate && eventDate.Before(startDate) {
			continue
		}
		afterCursor := eventTime.After(startTime) || (eventTime.Equal(startTime) && row.EventID > startAfterID)
		if hasAfter && !afterCursor {
			continue
		}
		if hasCutoff && eventTime.After(cutoff) {
			continue
		}
		if services != nil && !contains(services, row.ServiceName) {
			continue
		}
		if actions != nil && !contains(actions, row.ActionName) {
			continue
		}
		matched = append(matched, row)
	}

	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].EventTime.Equal(matched[j].EventTime) {
			return matched[i].EventTime.Before(matched[j].EventTime)
		}
		return matched[i].EventID < matched[j].EventID
	})
	if limit >= 0 && len(matched) > limit {
		matched = matched[:limit]
	}

	rows := make([][]*string, 0, len(matched))
	for _, row := range matched {
		values := make([]*string, len(cols))
		for i, col := range cols {
			values[i] = formatValue(row, col.Name)
		}
		rows = append(rows, values)
	}
	return cols, rows, nil
}

// formatValue renders a column value as JSON_ARRAY does.
// ASSUMPTION: TIMESTAMP as ISO 8601 UTC with milliseconds, DATE as yyyy-mm-dd, and MAP as a
// JSON object string.
func formatValue(row auditRow, name string) *string {
	var v string
	switch name {
	case "event_id":
		v = row.EventID
	case "event_time":
		v = row.EventTime.UTC().Format("2006-01-02T15:04:05.000Z")
	case "event_date":
		v = row.EventTime.UTC().Format(time.DateOnly)
	case "workspace_id":
		v = strconv.FormatInt(row.WorkspaceID, 10)
	case "service_name":
		v = row.ServiceName
	case "action_name":
		v = row.ActionName
	case "request_params":
		if row.RequestParams == nil {
			return nil
		}
		b, err := json.Marshal(row.RequestParams)
		if err != nil {
			return nil
		}
		v = string(b)
	default:
		return nil
	}
	return &v
}
