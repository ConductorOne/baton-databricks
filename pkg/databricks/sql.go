package databricks

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

const (
	statementsEndpoint = "/api/2.0/sql/statements"
	warehousesEndpoint = "/api/2.0/sql/warehouses"

	statementWaitTimeout = "30s"

	warehouseStateRunning  = "RUNNING"
	warehouseStateStarting = "STARTING"
)

// Vars so tests can shorten them.
var (
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

	res, polledRateLimit, err := c.pollStatement(ctx, workspaceId, warehouseId, res)
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
		// FAILED is usually a query/permission problem; CANCELED/CLOSED means it was stopped.
		code := codes.FailedPrecondition
		if res.Status.State != StatementStateFailed {
			code = codes.Aborted
		}
		return nil, rateLimit, uhttp.WrapErrors(code, fmt.Sprintf("statement %s did not succeed: state=%s message=%s", res.StatementID, res.Status.State, msg))
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
func (c *Client) pollStatement(ctx context.Context, workspaceId, warehouseId string, res statementResponse) (statementResponse, *v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	pollCtx, cancel := context.WithTimeout(ctx, statementPollMaxWait)
	defer cancel()

	var rateLimit *v2.RateLimitDescription
	for res.Status.State == StatementStatePending || res.Status.State == StatementStateRunning {
		select {
		case <-pollCtx.Done():
			return res, rateLimit, c.abandonStatement(ctx, workspaceId, warehouseId, res)
		case <-time.After(statementPollInterval):
		}

		l.Debug("polling databricks sql statement", zap.String("statement_id", res.StatementID), zap.String("state", string(res.Status.State)))

		u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, res.StatementID)
		var polled statementResponse
		// No cache: the URL is identical on every poll, so a cached PENDING/RUNNING
		// response would otherwise be returned until statementPollMaxWait.
		polledRateLimit, err := c.Get(WithoutCache(pollCtx), u, &polled)
		if polledRateLimit != nil {
			rateLimit = polledRateLimit
		}
		if err != nil {
			// The poll deadline can expire mid-request; that's still a timeout.
			if pollCtx.Err() != nil {
				return res, rateLimit, c.abandonStatement(ctx, workspaceId, warehouseId, res)
			}
			return res, rateLimit, fmt.Errorf("failed to poll statement %s: %w", res.StatementID, err)
		}
		res = polled
	}

	return res, rateLimit, nil
}

// abandonStatement cancels a statement that didn't finish in time and returns ctx's error if
// the caller gave up, or else a DeadlineExceeded error saying why the statement didn't finish.
func (c *Client) abandonStatement(ctx context.Context, workspaceId, warehouseId string, res statementResponse) error {
	c.cancelStatement(ctx, workspaceId, res.StatementID)
	if err := ctx.Err(); err != nil {
		return err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("sql statement did not reach a terminal state before poll timeout, canceled it",
		zap.String("statement_id", res.StatementID),
		zap.String("state", string(res.Status.State)),
		zap.Duration("max_wait", statementPollMaxWait),
	)

	msg := fmt.Sprintf("statement %s did not finish within %s (state=%s)", res.StatementID, statementPollMaxWait, res.Status.State)
	switch {
	case res.Status.State == StatementStateRunning:
		msg += ": the query was still running"
	case res.Status.State == StatementStatePending:
		// PENDING means waiting for compute, so the warehouse state says why.
		state, err := c.warehouseState(ctx, workspaceId, warehouseId)
		switch {
		case err != nil:
			l.Debug("failed to get sql warehouse state", zap.String("warehouse_id", warehouseId), zap.Error(err))
		case state == "":
		case state == warehouseStateRunning:
			msg += fmt.Sprintf(": SQL warehouse %s is running but did not pick up the statement; it may be at capacity", warehouseId)
		case state == warehouseStateStarting:
			msg += fmt.Sprintf(": SQL warehouse %s was still starting; a serverless warehouse starts much faster", warehouseId)
		default:
			msg += fmt.Sprintf(": SQL warehouse %s is %s and has not started; check its Monitoring tab for start failures", warehouseId, state)
		}
	}

	return uhttp.WrapErrors(codes.DeadlineExceeded, msg)
}

// warehouseState returns the warehouse's state (e.g. RUNNING, STARTING, STOPPED). Like
// cancelStatement, it ignores ctx's cancellation.
func (c *Client) warehouseState(ctx context.Context, workspaceId, warehouseId string) (string, error) {
	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	u := c.workspaceUrl(workspaceId).JoinPath(warehousesEndpoint, warehouseId)
	var res struct {
		State string `json:"state"`
	}
	if _, err := c.Get(WithoutCache(reqCtx), u, &res); err != nil {
		return "", err
	}
	return res.State, nil
}

// cancelStatement best-effort cancels a statement we've given up polling on. The request uses
// ctx's values (for the logger) but not its cancellation, since ctx/pollCtx may already be
// done. Uses /cancel, not DELETE (which only closes an already-terminal statement and
// wouldn't stop one still running).
func (c *Client) cancelStatement(ctx context.Context, workspaceId, statementId string) {
	l := ctxzap.Extract(ctx)

	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	u := c.workspaceUrl(workspaceId).JoinPath(statementsEndpoint, statementId, "cancel")
	response := struct{}{}
	if _, err := c.Post(reqCtx, u, nil, &response); err != nil {
		l.Debug("failed to cancel timed-out sql statement", zap.String("statement_id", statementId), zap.Error(err))
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

// WarehouseExists reports whether warehouseId exists in workspaceId, since SQL warehouses
// are workspace-scoped with no account-level lookup.
func (c *Client) WarehouseExists(ctx context.Context, workspaceId, warehouseId string) (bool, *v2.RateLimitDescription, error) {
	u := c.workspaceUrl(workspaceId).JoinPath(warehousesEndpoint, warehouseId)

	var res struct{}
	rateLimit, err := c.Get(ctx, u, &res)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return false, rateLimit, nil
		}
		return false, rateLimit, err
	}

	return true, rateLimit, nil
}
