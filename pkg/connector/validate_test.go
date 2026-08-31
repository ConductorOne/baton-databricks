package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/conductorone/baton-databricks/pkg/databricks"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type rolesOKTransport struct{}

func (rolesOKTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"roles":[]}`)),
		Request:    req,
	}, nil
}

// captureLogs returns a ctx carrying a zap logger that writes JSON lines to buf.
func captureLogs(ctx context.Context, buf *bytes.Buffer) context.Context {
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(buf),
		zapcore.DebugLevel,
	)
	return ctxzap.ToContext(ctx, zap.New(core))
}

func newValidateConnector(t *testing.T, auth databricks.Auth) *Databricks {
	t.Helper()
	httpClient := &http.Client{Transport: rolesOKTransport{}}
	client, err := databricks.NewClient(
		context.Background(), httpClient,
		"cloud.databricks.com", "accounts.cloud.databricks.com",
		"acct-1", "", auth, nil,
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return &Databricks{client: client, workspaces: []string{"ws1"}}
}

// levelFor scans the captured JSON log lines for the first entry whose message
// contains want and returns its level. Empty string means no such entry.
func levelFor(t *testing.T, buf *bytes.Buffer, want string) string {
	t.Helper()
	for _, line := range strings.Split(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var entry struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if strings.Contains(entry.Msg, want) {
			return entry.Level
		}
	}
	return ""
}

const accountUnreachableMsg = "account API unreachable"

// CXH-2350: under workspace-token auth the account plane is unreachable but the
// sync still proceeds workspace-scoped. That reduced-coverage notice is an operator
// diagnostic and must log at debug, not warn (repo convention forbids warn).
func TestValidateWorkspaceTokenLogsAtDebug(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := captureLogs(context.Background(), buf)
	d := newValidateConnector(t, databricks.NewTokenAuth([]string{"ws1"}, []string{"tok"}))

	if _, err := d.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := levelFor(t, buf, accountUnreachableMsg); got != "debug" {
		t.Errorf("account-unreachable notice logged at %q, want %q", got, "debug")
	}
}

// When the account API is reachable (non-token auth), the notice must not fire at all.
func TestValidateAccountReachableNoNotice(t *testing.T) {
	buf := &bytes.Buffer{}
	ctx := captureLogs(context.Background(), buf)
	d := newValidateConnector(t, &databricks.NoAuth{})

	if _, err := d.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if got := levelFor(t, buf, accountUnreachableMsg); got != "" {
		t.Errorf("account-unreachable notice fired (level %q) when account API was reachable", got)
	}
}
