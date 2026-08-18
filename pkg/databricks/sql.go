package databricks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const (
	statementsEndpoint = "/api/2.0/sql/statements"

	statementWaitTimeout  = "30s"
	statementPollInterval = 2 * time.Second
)

type StatementState string

const (
	StatementStatePending   StatementState = "PENDING"
	StatementStateRunning   StatementState = "RUNNING"
	StatementStateSucceeded StatementState = "SUCCEEDED"
	StatementStateFailed    StatementState = "FAILED"
	StatementStateCanceled  StatementState = "CANCELED"
	StatementStateClosed    StatementState = "CLOSED"
)

// StatementParameter binds a named parameter referenced as ":name" in a SQL statement.
type StatementParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"`
}

type statementRequestBody struct {
	WarehouseID string               `json:"warehouse_id"`
	Statement   string               `json:"statement"`
	WaitTimeout string               `json:"wait_timeout,omitempty"`
	Format      string               `json:"format"`
	Disposition string               `json:"disposition"`
	Parameters  []StatementParameter `json:"parameters,omitempty"`
}

type statementError struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

type statementStatus struct {
	State StatementState  `json:"state"`
	Error *statementError `json:"error,omitempty"`
}

type statementManifest struct {
	Schema struct {
		Columns []struct {
			Name string `json:"name"`
		} `json:"columns"`
	} `json:"schema"`
}

type statementResultChunk struct {
	NextChunkIndex *int       `json:"next_chunk_index,omitempty"`
	DataArray      [][]string `json:"data_array"`
}

type statementResponse struct {
	StatementID string               `json:"statement_id"`
	Status      statementStatus      `json:"status"`
	Manifest    statementManifest    `json:"manifest"`
	Result      statementResultChunk `json:"result"`
}

// StatementResult is the flattened result of a SQL statement executed via the
// Statement Execution API, with all result chunks already collected.
type StatementResult struct {
	Columns []string
	Rows    [][]string
}

// ExecuteStatement runs a SQL statement via the Statement Execution API and returns every row.
func (c *Client) ExecuteStatement(
	ctx context.Context,
	workspaceId string,
	warehouseId string,
	statement string,
	params ...StatementParameter,
) (*StatementResult, error) {
	u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint)

	body := statementRequestBody{
		WarehouseID: warehouseId,
		Statement:   statement,
		WaitTimeout: statementWaitTimeout,
		Format:      "JSON_ARRAY",
		Disposition: "INLINE",
		Parameters:  params,
	}

	var res statementResponse
	if _, err := c.Post(ctx, u, body, &res); err != nil {
		return nil, fmt.Errorf("failed to submit statement: %w", err)
	}

	res, err := c.pollStatement(ctx, workspaceId, res)
	if err != nil {
		return nil, err
	}

	switch res.Status.State {
	case StatementStateSucceeded:
	default:
		msg := ""
		if res.Status.Error != nil {
			msg = res.Status.Error.Message
		}
		return nil, fmt.Errorf("statement %s did not succeed: state=%s message=%s", res.StatementID, res.Status.State, msg)
	}

	return c.collectStatementResult(ctx, workspaceId, res)
}

// pollStatement blocks until the statement reaches a terminal state, for the case of a
// cold warehouse start still running after the initial statementWaitTimeout.
func (c *Client) pollStatement(ctx context.Context, workspaceId string, res statementResponse) (statementResponse, error) {
	l := ctxzap.Extract(ctx)

	for res.Status.State == StatementStatePending || res.Status.State == StatementStateRunning {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(statementPollInterval):
		}

		l.Debug("polling databricks sql statement", zap.String("statement_id", res.StatementID), zap.String("state", string(res.Status.State)))

		u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, res.StatementID)
		var polled statementResponse
		if _, err := c.Get(ctx, u, &polled); err != nil {
			return res, fmt.Errorf("failed to poll statement %s: %w", res.StatementID, err)
		}
		res = polled
	}

	return res, nil
}

func (c *Client) collectStatementResult(ctx context.Context, workspaceId string, res statementResponse) (*StatementResult, error) {
	columns := make([]string, len(res.Manifest.Schema.Columns))
	for i, col := range res.Manifest.Schema.Columns {
		columns[i] = col.Name
	}

	rows := make([][]string, 0, len(res.Result.DataArray))
	rows = append(rows, res.Result.DataArray...)

	nextChunk := res.Result.NextChunkIndex
	for nextChunk != nil {
		u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, res.StatementID, "result", "chunks", strconv.Itoa(*nextChunk))
		var chunk statementResultChunk
		if _, err := c.Get(ctx, u, &chunk); err != nil {
			return nil, fmt.Errorf("failed to fetch statement result chunk %d: %w", *nextChunk, err)
		}
		rows = append(rows, chunk.DataArray...)
		nextChunk = chunk.NextChunkIndex
	}

	return &StatementResult{Columns: columns, Rows: rows}, nil
}

// ValidateAuditLogAccess confirms the configured warehouse can query system.access.audit,
// which requires a one-time SELECT grant from a metastore admin (see README).
func (c *Client) ValidateAuditLogAccess(ctx context.Context, workspaceId, warehouseId string) error {
	if _, err := c.ExecuteStatement(ctx, workspaceId, warehouseId, "SELECT 1 FROM system.access.audit LIMIT 1"); err != nil {
		return fmt.Errorf("failed to query system.access.audit: %w", err)
	}
	return nil
}
