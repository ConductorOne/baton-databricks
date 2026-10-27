package databricks

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rewriteHostTransport struct{ target *url.URL }

func (t rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = t.target.Scheme
	req.URL.Host = t.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newSQLTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	c, err := NewClient(context.Background(), &http.Client{Transport: rewriteHostTransport{target: target}},
		"example.cloud.databricks.com", "accounts.cloud.databricks.com", "acc-1", "", &NoAuth{}, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func shortenStatementPolling(t *testing.T) {
	t.Helper()
	interval, maxWait := statementPollInterval, statementPollMaxWait
	statementPollInterval, statementPollMaxWait = 10*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { statementPollInterval, statementPollMaxWait = interval, maxWait })
}

func TestExecuteStatementTimeoutExplainsWarehouseState(t *testing.T) {
	shortenStatementPolling(t)

	cases := []struct {
		name           string
		statementState StatementState
		warehouseState string
		want           string
	}{
		{"stopped warehouse", StatementStatePending, "STOPPED", "SQL warehouse wh-1 is STOPPED and has not started"},
		{"starting warehouse", StatementStatePending, "STARTING", "SQL warehouse wh-1 was still starting"},
		{"running warehouse", StatementStatePending, "RUNNING", "SQL warehouse wh-1 is running but did not pick up the statement"},
		{"running query", StatementStateRunning, "RUNNING", "the query was still running"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var canceled atomic.Bool
			c := newSQLTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/cancel"):
					canceled.Store(true)
					fmt.Fprint(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/sql/warehouses/wh-1"):
					fmt.Fprintf(w, `{"id":"wh-1","state":%q}`, tc.warehouseState)
				default:
					fmt.Fprintf(w, `{"statement_id":"stmt-1","status":{"state":%q}}`, tc.statementState)
				}
			})

			_, _, err := c.ExecuteStatement(context.Background(), "dbc-aaa", "wh-1", "SELECT 1")
			if err == nil {
				t.Fatal("ExecuteStatement() error = nil, want a timeout")
			}
			if got := status.Code(err); got != codes.DeadlineExceeded {
				t.Errorf("error code = %s, want %s", got, codes.DeadlineExceeded)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
			if !canceled.Load() {
				t.Error("statement was not canceled")
			}
		})
	}
}

// A poll that is still in flight when the deadline expires must also cancel the statement.
func TestExecuteStatementTimeoutDuringPollCancelsStatement(t *testing.T) {
	shortenStatementPolling(t)

	var canceled atomic.Bool
	c := newSQLTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/cancel"):
			canceled.Store(true)
			fmt.Fprint(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/sql/warehouses/wh-1"):
			fmt.Fprint(w, `{"id":"wh-1","state":"STOPPED"}`)
		case r.Method == http.MethodGet:
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		default:
			fmt.Fprint(w, `{"statement_id":"stmt-1","status":{"state":"PENDING"}}`)
		}
	})

	_, _, err := c.ExecuteStatement(context.Background(), "dbc-aaa", "wh-1", "SELECT 1")
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("error = %v, want code %s", err, codes.DeadlineExceeded)
	}
	if !strings.Contains(err.Error(), "has not started") {
		t.Errorf("error = %q, want the warehouse state explained", err)
	}
	if !canceled.Load() {
		t.Error("statement was not canceled")
	}
}
