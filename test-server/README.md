# Databricks test server

A local stand-in for the Databricks APIs that baton-databricks calls from `Validate` and the
incremental sync event feed (`databricks_audit_log`, which reads `system.access.audit` through the
SQL Statement Execution API). It lets you run the event feed end to end without a Databricks
account, locally and in CI.

Go standard library only, built from the root module. State is in memory, seeded at startup and
reset on every start. Full sync and provisioning endpoints are not implemented yet.

## Running it

```bash
# Start the server from the repo root; it writes its CA certificate to ./test-server-ca.pem
go run ./test-server/

# Build the connector and run the event feed against it; events are printed to stdout as JSON lines
go build ./cmd/baton-databricks
SSL_CERT_FILE=./test-server-ca.pem ./baton-databricks \
  --hostname localhost:8765 --account-hostname localhost:8765 \
  --account-id test-account \
  --databricks-client-id test-client --databricks-client-secret test-secret \
  --sql-warehouse-id wh-serverless \
  --event-feed true --event-feed-id databricks_audit_log \
  --event-feed-start-at "$(date -u -d '2 days ago' +%Y-%m-%dT%H:%M:%SZ)"
```

`--event-feed true` is what selects the event feed mode; `--event-feed-id` alone doesn't. The
connector's logs go to stderr, so stdout holds only events.

To compare with the expected output (event times depend on when the server started, so
`occurredAt` is dropped):

```bash
normalize() { jq -c -S 'del(.occurredAt)' "$1" | sort; }
diff <(normalize test-server/testdata/expected-events.jsonl) <(normalize events.jsonl)
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--addr` | `localhost:8765` | Listen address |
| `--ca-out` | `./test-server-ca.pem` | Where to write the generated CA certificate |
| `--warehouse-start-delay` | `0s` | How long a stopped warehouse takes to reach `RUNNING` once a statement starts it |
| `--chunk-rows` | `0` | Rows per result chunk; `0` returns every row in one chunk |

### TLS and hosts

The connector always builds `https://` URLs, so the server serves TLS. At startup it creates a
throwaway CA and a certificate for `localhost`, `*.localhost` and each seeded workspace host
(`ws-alpha.localhost`, ...), and writes the CA to `--ca-out`. The connector trusts it through
`SSL_CERT_FILE`. The workspace hosts are listed because OpenSSL, and so curl, doesn't match a
`*.localhost` wildcard; Go does.

The server routes by `Host` header:

- `localhost:8765` is the account API and OAuth host (`--account-hostname`).
- `<deployment_name>.localhost:8765` is that workspace (the connector builds
  `<deployment_name>.<hostname>`). Go resolves any `*.localhost` name to loopback, so no DNS setup
  is needed. An unknown deployment name gets a 404.

**macOS:** Go ignores `SSL_CERT_FILE` on macOS and uses the keychain. Either trust the CA in the
keychain or run the connector in a Linux container.

### Other warehouses

- `--sql-warehouse-id wh-pro` reads the same events through a stopped warehouse in `ws-beta`.
  The first statement starts it. With `--warehouse-start-delay 35s` the connector's first wait (30s)
  times out and it then polls until the warehouse is running.
- `--sql-warehouse-id wh-broken` never starts. The connector gives up after its 5-minute limit,
  cancels the statement, and fails with "SQL warehouse wh-broken is STOPPED and has not started".

## Auth

| | Value |
|---|---|
| Account ID | `test-account` |
| Client ID / secret | `test-client` / `test-secret` |
| Access token | `test-access-token` |

The token endpoint takes the client ID and secret as HTTP Basic auth, with form fields
`grant_type=client_credentials` and `scope=all-apis`. Every other endpoint (except `/health`)
needs `Authorization: Bearer test-access-token` and returns 401
`{"error_code": "UNAUTHENTICATED", ...}` without it.

## Endpoints

| Host | Method | Path | Docs |
|---|---|---|---|
| account | POST | `/oidc/accounts/{account_id}/v1/token` | [OAuth M2M](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m) |
| account | GET | `/api/2.0/preview/accounts/{account_id}/access-control/assignable-roles` | [Account access control](https://docs.databricks.com/api/account/accountaccesscontrol/getassignablerolesforresource) |
| account | GET | `/api/2.0/accounts/{account_id}/workspaces` | [List workspaces](https://docs.databricks.com/api/account/workspaces/list) |
| account | GET | `/api/2.0/accounts/{account_id}/scim/v2/Users/{id}` | [Get account user](https://docs.databricks.com/api/account/accountusers/get) |
| account | GET | `/api/2.0/accounts/{account_id}/scim/v2/Groups/{id}` | [Get account group](https://docs.databricks.com/api/account/accountgroups/get) |
| account | GET | `/api/2.0/accounts/{account_id}/scim/v2/ServicePrincipals/{id}` | [Get account service principal](https://docs.databricks.com/api/account/accountserviceprincipals/get) |
| workspace | GET | `/api/2.0/sql/warehouses/{id}` | [Get a warehouse](https://docs.databricks.com/api/workspace/warehouses/get) |
| workspace | POST | `/api/2.0/sql/statements` | [Execute a statement](https://docs.databricks.com/api/workspace/statementexecution/executestatement) |
| workspace | GET | `/api/2.0/sql/statements/{id}` | [Get a statement](https://docs.databricks.com/api/workspace/statementexecution/getstatement) |
| workspace | POST | `/api/2.0/sql/statements/{id}/cancel` | [Cancel a statement](https://docs.databricks.com/api/workspace/statementexecution/cancelexecution) |
| workspace | GET | `/api/2.0/sql/statements/{id}/result/chunks/{n}` | [Get result chunk](https://docs.databricks.com/api/workspace/statementexecution/getstatementresultchunkn) |
| any | GET | `/health` | None: local readiness check |

Every route in `handlers.go` links its doc page.

## Behavior and provenance

Code comments label each behavior:

- `DOC`: stated in the official Databricks docs (with a link).
- `ASSUMPTION`: not documented; our best guess at the real behavior.
- `DECISION`: a choice made for this server, with no real-world counterpart.

### From the docs

- Token endpoint: Basic client credentials, `grant_type=client_credentials`, `scope=all-apis`;
  returns `{access_token, token_type: "Bearer", expires_in: 3600}`.
- API errors use `{error_code, message}`; a missing or wrong token is 401 `UNAUTHENTICATED`.
- The workspaces list is a bare JSON array (`workspace_id` int64, `workspace_name`,
  `deployment_name`, `aws_region`, `workspace_status`, ...).
- SCIM user, group and service principal objects and their fields.
- Warehouse fields and `state` values (`STARTING`, `RUNNING`, `STOPPED`, ...). A statement on a
  warehouse that isn't running stays `PENDING`.
- `wait_timeout` is `0s` or `5s`–`50s` (default `10s`); anything else is 400
  `INVALID_PARAMETER_VALUE`. `on_wait_timeout` is `CONTINUE` (default) or `CANCEL`.
- A statement that finishes within `wait_timeout` returns its result; otherwise the response has
  its ID and `PENDING`. GET returns the manifest and first chunk once `SUCCEEDED`. Cancel returns
  `{}` and the statement becomes `CANCELED`.
- `data_array` values are strings, nulls are JSON `null`, and `next_chunk_index` is absent on the
  last chunk. Column `type_name` values (`STRING`, `TIMESTAMP`, `LONG`, `MAP`, ...).
- Parameters are named `:name` markers with an optional `type` (default `STRING`).
- `system.access.audit` columns; account-level events have `workspace_id = 0`; the
  `request_params` keys of each event ([audit log reference](https://docs.databricks.com/aws/en/admin/account-settings/audit-logs)),
  including workspace-level `createGroup` without `targetGroupId` and `updatePermissionAssignment`
  without `workspace_id`.
- Regionality: "Most audit logs are only available in the region of the workspace"
  ([system tables: audit logs](https://docs.databricks.com/aws/en/admin/system-tables/audit-logs)).
  Here, a warehouse sees workspace-level rows only from workspaces in its own region, and
  account-level rows from every region.

### What's not from the docs

| Kind | Behavior |
|---|---|
| `ASSUMPTION` | Token endpoint failures use the RFC 6749 body `{error, error_description}` (401 bad client, 400 bad grant or scope). |
| `ASSUMPTION` | An account ID other than `test-account` gets 404 `RESOURCE_DOES_NOT_EXIST`. |
| `ASSUMPTION` | SCIM 404 body uses the SCIM error schema (`schemas`, `detail`, `status: "404"`). |
| `ASSUMPTION` | Unknown API paths get 404 `ENDPOINT_NOT_FOUND`. |
| `ASSUMPTION` | A warehouse that doesn't exist in the request's workspace is 404 `RESOURCE_DOES_NOT_EXIST`, both for GET warehouse and for `warehouse_id` on a statement. The docs only say it's an error. |
| `ASSUMPTION` | A positional (unnamed) parameter is 400 `INVALID_PARAMETER_VALUE`. |
| `ASSUMPTION` | Submitting a statement starts a stopped warehouse, which is `RUNNING` after `--warehouse-start-delay`. |
| `ASSUMPTION` | `JSON_ARRAY` value formats: `TIMESTAMP` as `2006-01-02T15:04:05.000Z` (UTC, milliseconds), `DATE` as `2006-01-02`, `MAP` as a JSON object string. |
| `ASSUMPTION` | An empty result is one chunk with `row_count: 0` and no `data_array`. |
| `ASSUMPTION` | Unknown statement IDs are 404 `RESOURCE_DOES_NOT_EXIST`; an out-of-range chunk or a chunk of a statement without a result is 400. |
| `ASSUMPTION` | An unbound `:name` marker fails the statement with `[UNBOUND_SQL_PARAMETER]`. Accepted `TIMESTAMP` parameter formats: RFC 3339 or `yyyy-mm-dd hh:mm:ss[.ffffff]`. |
| `DECISION` | Hardcoded credentials, account ID and token. |
| `DECISION` | TLS with a generated CA; routing by `Host` header; `/health`. |
| `DECISION` | Assignable roles returns a fixed list. |
| `DECISION` | Seed warehouses: `wh-serverless` running, `wh-pro` stopped, `wh-broken` never starts (stays `STOPPED` with `health.status: FAILED`). |
| `DECISION` | Statements go straight from `PENDING` to `SUCCEEDED`/`FAILED` once the warehouse runs; there's no visible `RUNNING` state. |
| `DECISION` | One result chunk unless `--chunk-rows` is set; only `format: JSON_ARRAY` and `disposition: INLINE` (no `EXTERNAL_LINKS`); no 12-hour statement expiry. |
| `DECISION` | SQL handling (below). |
| `DECISION` | Seed data is synthetic (below). |

### SQL handling (`DECISION`)

The server doesn't run SQL. It recognizes `SELECT <columns> FROM system.access.audit` and:

- binds the typed `:name` parameters;
- applies the predicates the connector's query uses, recognized by shape:
  `event_date >= :d`, `(event_time > :t OR (event_time = :t AND event_id > :id))`,
  `event_time <= :cutoff`, `service_name IN (...)` and `action_name IN (...)`;
- requires `ORDER BY event_time ASC, event_id ASC` and applies a trailing `LIMIT n`;
- fails any other statement with `FAILED` and "test-server: unsupported statement".

This is deliberately tied to the shape of the connector's audit query
(`pkg/connector/audit_event_feed.go`). If that query changes shape, update `statements.go`.

## Seed data

All IDs and times are made up. Times are relative to server start.

**Workspaces**

| `deployment_name` | `workspace_id` | Region |
|---|---|---|
| `ws-alpha` | 7474645012345671 | us-east-1 |
| `ws-beta` | 7474645012345672 | us-east-1 |
| `ws-gamma` | 8828000011112223 | eu-west-1 |

**Principals**

| Kind | ID | Name |
|---|---|---|
| User | 4410000000000001 | alice@example.com |
| User | 4410000000000002 | bob@example.com |
| User | 4410000000000003 | carol@example.com (inactive) |
| User | 4410000000000004 | dave@example.com |
| Group | 5520000000000001 | data-engineers |
| Group | 5520000000000002 | analysts |
| Service principal | 6630000000000001 | ci-bot |
| None (deleted principal) | 9990000000000001 | matches nothing |

**Warehouses**

| ID | Workspace | State |
|---|---|---|
| `wh-serverless` | `ws-alpha` | `RUNNING` (serverless) |
| `wh-pro` | `ws-beta` | `STOPPED`; starts on the first statement |
| `wh-broken` | `ws-alpha` | `STOPPED`; never starts |

**Audit rows and the events they should produce**

Rows the connector should read fall between 26 and 5 hours before server start: older than its
4-hour lag window and inside an `--event-feed-start-at` of 2 days ago. Event IDs are
`<event_id>/<n>`, and seed event IDs are `e0000001-0000-4000-8000-0000000000NN`. The expected
events, as protojson, are in `testdata/expected-events.jsonl`. Every principal is parented to the
account, as in full sync.

| # | Time | Workspace | `service_name` / `action_name` | Key `request_params` | Expected events |
|---|---|---|---|---|---|
| 1 | −26h | 0 | accounts / `add` | targetUserId dave | dave changed |
| 2 | −25h | 0 | accounts / `activateUser` | targetUserId bob | bob changed |
| 3 | −24h | 0 | accounts / `deactivateUser` | targetUserId carol | carol changed |
| 4 | −23h | 0 | accounts / `createGroup` | targetGroupId analysts | analysts changed |
| 5 | −22h | 0 | accounts / `addPrincipalToGroup` | data-engineers ← alice | grant: alice gets data-engineers `member` |
| 6 | −21h | 0 | accounts / `addPrincipalToGroup` | data-engineers ← analysts | grant: group analysts gets data-engineers `member` |
| 7 | −20h | 0 | accounts / `addPrincipalsToGroup` | analysts ← ci-bot | grant: service principal ci-bot gets analysts `member` |
| 8 | −19h | 0 | accounts / `removePrincipalFromGroup` | data-engineers ← bob | revoke: bob loses data-engineers `member` |
| 9 | −18h | 0 | accounts / `removePrincipalFromGroup` | data-engineers ← deleted principal | data-engineers changed (member can't be resolved) |
| 10 | −17h | 0 | accounts / `setAccountAdmin` | targetUserId alice | alice changed; `account_admin` role changed |
| 11 | −16h | ws-alpha | accounts / `setAdmin` | targetUserId bob | bob changed; `account_admin` role changed (the connector refreshes it in case `setAdmin` is account admin, which the reference is ambiguous about) |
| 12 | −15h | ws-alpha | accounts / `removeAdmin` | targetUserId bob | bob changed |
| 13 | −14h | ws-alpha | accounts / `createGroup` (workspace-level, no `targetGroupId`) | targetUserId alice | none |
| 14 | −13h | ws-beta | accounts / `delete` | targetUserId carol | carol changed |
| 15 | −12h | 0 | workspace / `addPermissionAssignment` | workspace_id ws-alpha | ws-alpha changed |
| 16 | −11h | 0 | workspace / `updatePermissionAssignment` (no `workspace_id`) | principal_id dave | none |
| 17 | −10h | ws-beta | workspace / `deletePermissionAssignment` | workspace_id ws-beta | ws-beta changed |
| 18 | −9h | ws-gamma | accounts / `add` | targetUserId alice | none: eu-west-1 isn't visible from a us-east-1 warehouse |
| 19 | −8h | ws-alpha | accounts / `updateUser` | targetUserId alice | alice changed; `ws-alpha:allow-cluster-create` and `ws-alpha:allow-instance-pool-create` changed |
| 20 | −7h | 0 | accounts / `updateGroup` | targetGroupId analysts | analysts changed |
| 21 | −6h | 0 | accounts / `removeGroup` | targetGroupId data-engineers | data-engineers changed |
| 22 | −6h (same time as 21) | 0 | accounts / `deactivateUser` | targetUserId dave | dave changed |
| 23 | −5h30m | ws-alpha | accounts / `changeDatabricksWorkspaceAcl` | targetUserId carol | ws-alpha changed; `ws-alpha:workspace-access` changed |
| 24 | −5h | ws-alpha | clusters / `create` | | none: not requested |
| 25 | −2h | 0 | accounts / `add` | targetUserId bob | none: newer than the 4-hour lag cutoff |
| 26 | −72h | 0 | accounts / `add` | targetUserId carol | none: older than the feed start |

That adds up to 25 events.

## curl examples

```bash
CA="--cacert ./test-server-ca.pem"

curl -s $CA https://localhost:8765/health

TOKEN=$(curl -s $CA -u test-client:test-secret \
  -d grant_type=client_credentials -d scope=all-apis \
  https://localhost:8765/oidc/accounts/test-account/v1/token | jq -r .access_token)

curl -s $CA -H "Authorization: Bearer $TOKEN" https://localhost:8765/api/2.0/accounts/test-account/workspaces | jq
curl -s $CA -H "Authorization: Bearer $TOKEN" https://localhost:8765/api/2.0/accounts/test-account/scim/v2/Users/4410000000000001 | jq
curl -s $CA -H "Authorization: Bearer $TOKEN" https://ws-alpha.localhost:8765/api/2.0/sql/warehouses/wh-serverless | jq

curl -s $CA -H "Authorization: Bearer $TOKEN" https://ws-alpha.localhost:8765/api/2.0/sql/statements \
  -d '{
    "warehouse_id": "wh-serverless",
    "statement": "SELECT event_id, event_time, action_name FROM system.access.audit WHERE action_name IN ('\''add'\'') ORDER BY event_time ASC, event_id ASC LIMIT 10",
    "wait_timeout": "10s"
  }' | jq
```

## Tests and CI

`main_test.go` tests the server in process: auth, routing, `wait_timeout` validation, warehouse
and statement state transitions (including the stopped and broken warehouses), cancel and
`on_wait_timeout: CANCEL`, chunking, regionality and the audit filter. It runs with
`go test ./...`.

The `test-server` job in `.github/workflows/ci.yaml` starts the server, runs the event feed
against `wh-serverless` and diffs the events with `testdata/expected-events.jsonl`. It then runs
against `wh-broken` and checks for the "has not started" message. That step waits out the
connector's 5-minute statement limit. The job needs no secrets.

If a diff shows up, either the connector or the expectation is wrong; check the row's documented
meaning in the table above before updating `expected-events.jsonl`.
