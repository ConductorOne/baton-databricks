package databricks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const (
	statementsEndpoint = "/api/2.0/sql/statements"

	statementWaitTimeout  = "30s"
	statementPollInterval = 2 * time.Second
	statementPollMaxWait  = 5 * time.Minute
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

// ExecuteStatement runs a SQL statement via the Statement Execution API and returns every
// row, along with the rate-limit info from the last call that reported any.
func (c *Client) ExecuteStatement(
	ctx context.Context,
	workspaceId string,
	warehouseId string,
	statement string,
	params ...StatementParameter,
) (*StatementResult, *v2.RateLimitDescription, error) {
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
	rateLimit, err := c.Post(ctx, u, body, &res)
	if err != nil {
		return nil, rateLimit, fmt.Errorf("failed to submit statement: %w", err)
	}

	res, polledRateLimit, err := c.pollStatement(ctx, workspaceId, res)
	if polledRateLimit != nil {
		rateLimit = polledRateLimit
	}
	if err != nil {
		return nil, rateLimit, err
	}

	if res.Status.State != StatementStateSucceeded {
		msg := ""
		if res.Status.Error != nil {
			msg = res.Status.Error.Message
		}
		return nil, rateLimit, fmt.Errorf("statement %s did not succeed: state=%s message=%s", res.StatementID, res.Status.State, msg)
	}

	result, resultRateLimit, err := c.collectStatementResult(ctx, workspaceId, res)
	if resultRateLimit != nil {
		rateLimit = resultRateLimit
	}
	return result, rateLimit, err
}

// pollStatement blocks until the statement reaches a terminal state, for the case of a
// cold warehouse start still running after the initial statementWaitTimeout. Capped at
// statementPollMaxWait so a warehouse stuck PENDING/RUNNING can't hang the caller
// indefinitely; on giving up (or on ctx cancellation) it cancels the statement so it stops
// occupying the warehouse.
func (c *Client) pollStatement(ctx context.Context, workspaceId string, res statementResponse) (statementResponse, *v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	pollCtx, cancel := context.WithTimeout(ctx, statementPollMaxWait)
	defer cancel()

	var rateLimit *v2.RateLimitDescription
	for res.Status.State == StatementStatePending || res.Status.State == StatementStateRunning {
		select {
		case <-pollCtx.Done():
			if err := ctx.Err(); err != nil {
				c.cancelStatement(workspaceId, res.StatementID)
				return res, rateLimit, err
			}
			l.Warn("sql statement did not reach a terminal state before poll timeout, canceling",
				zap.String("statement_id", res.StatementID),
				zap.String("state", string(res.Status.State)),
				zap.Duration("max_wait", statementPollMaxWait),
			)
			c.cancelStatement(workspaceId, res.StatementID)
			return res, rateLimit, fmt.Errorf("statement %s did not reach a terminal state within %s", res.StatementID, statementPollMaxWait)
		case <-time.After(statementPollInterval):
		}

		l.Debug("polling databricks sql statement", zap.String("statement_id", res.StatementID), zap.String("state", string(res.Status.State)))

		u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, res.StatementID)
		var polled statementResponse
		polledRateLimit, err := c.Get(pollCtx, u, &polled)
		if polledRateLimit != nil {
			rateLimit = polledRateLimit
		}
		if err != nil {
			return res, rateLimit, fmt.Errorf("failed to poll statement %s: %w", res.StatementID, err)
		}
		res = polled
	}

	return res, rateLimit, nil
}

// cancelStatement best-effort cancels a statement we've given up polling on, using a fresh
// context since ctx/pollCtx may already be done.
func (c *Client) cancelStatement(workspaceId, statementId string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, statementId)
	if _, err := c.Delete(ctx, u); err != nil {
		ctxzap.Extract(ctx).Warn("failed to cancel timed-out sql statement", zap.String("statement_id", statementId), zap.Error(err))
	}
}

func (c *Client) collectStatementResult(ctx context.Context, workspaceId string, res statementResponse) (*StatementResult, *v2.RateLimitDescription, error) {
	columns := make([]string, len(res.Manifest.Schema.Columns))
	for i, col := range res.Manifest.Schema.Columns {
		columns[i] = col.Name
	}

	rows := make([][]string, 0, len(res.Result.DataArray))
	rows = append(rows, res.Result.DataArray...)

	var rateLimit *v2.RateLimitDescription
	nextChunk := res.Result.NextChunkIndex
	for nextChunk != nil {
		u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, res.StatementID, "result", "chunks", strconv.Itoa(*nextChunk))
		var chunk statementResultChunk
		chunkRateLimit, err := c.Get(ctx, u, &chunk)
		if chunkRateLimit != nil {
			rateLimit = chunkRateLimit
		}
		if err != nil {
			return nil, rateLimit, fmt.Errorf("failed to fetch statement result chunk %d: %w", *nextChunk, err)
		}
		rows = append(rows, chunk.DataArray...)
		nextChunk = chunk.NextChunkIndex
	}

	return &StatementResult{Columns: columns, Rows: rows}, rateLimit, nil
}

// ValidateAuditLogAccess confirms the configured warehouse can query system.access.audit,
// which requires a one-time SELECT grant from a metastore admin (see README).
func (c *Client) ValidateAuditLogAccess(ctx context.Context, workspaceId, warehouseId string) error {
	if _, _, err := c.ExecuteStatement(ctx, workspaceId, warehouseId, "SELECT 1 FROM system.access.audit LIMIT 1"); err != nil {
		return fmt.Errorf("failed to query system.access.audit: %w", err)
	}
	return nil
}
