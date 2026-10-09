While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.
   >
   > It also syncs Unity Catalog metastores. That resource type is opt-in — it stays off until it is enabled per resource type in ConductorOne.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   > - **Unity Catalog privileges**: grant and revoke a metastore privilege for a user, group or service principal. The `owner` entitlement is read-only; granting or revoking it returns `InvalidArgument`.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID and OAuth credentials:
   >
   > - **OAuth**: a service principal OAuth client ID and client secret. Syncs the account and every workspace the service principal can access. This is the connector's only authentication method.
   >
   > Google Cloud Platform and Azure Databricks customers also provide the account hostname and hostname.

2. For each item in the list above:

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process.

     > - **Account ID**: in the Databricks account console, open the menu next to your username in the upper-right corner; the account ID is shown there.
     > - **OAuth client ID and secret**: follow the [Databricks OAuth (M2M) documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a service principal and generate an OAuth secret.
     > - **Deployment name**: the subdomain in the workspace URL (not the numeric workspace ID).

   * Does the credential need any specific scopes or permissions? If so, list them here.

     > The credential must have admin access to each resource it reads or writes: account-admin on the Databricks account (for account-level sync and provisioning) and workspace-admin on each workspace being synced.
     >
     > The OAuth token carries scopes on top of that. The connector requests `all-apis`, which covers the four it uses: `unity-catalog` (the securables and the permissions endpoint), `scim` (account principals), `access-management` (roles and workspace permission assignments) and `provisioning` (the account workspaces listing). The scope-by-scope breakdown is in `## Permission model` below.
     >
     > Unity Catalog adds a third layer on top of scopes and roles: standing on the securable itself. For each metastore being synced, the service principal must either own the metastore or be an admin of a workspace it is assigned to. A privilege read cannot settle it at this level, because Databricks rejects `MANAGE` and `ALL_PRIVILEGES` on a metastore. `## Permission model` explains why neither is optional.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate scopes: Databricks admin access covers both read (sync) and read-write (provision).

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Unity Catalog API

A metastore lives on the account plane, but its privileges are only readable
through a workspace host assigned to it. The connector enumerates metastores from
the account, resolves which workspaces are assigned to each one, and issues the
permissions calls against one of those workspace hosts.

The catalog listing is read here for one purpose only: Databricks answers the
workspace metastore-assignment endpoint with 404 both for a workspace outside
Unity Catalog and for an assignment the credential may not read, and listing a
workspace's catalogs is what separates the two. The catalogs themselves are not
synced by this release.

| Level | Endpoint |
|-------|----------|
| Metastores | `GET /api/2.0/accounts/{account_id}/metastores` |
| Workspace metastore assignment | `GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/metastore` |
| Catalogs (access-path resolution only) | `GET /api/2.1/unity-catalog/catalogs` |
| Privileges | `GET` and `PATCH` `/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` |

Behaviours that shape the implementation:

- **The permissions endpoint is the only one that returns `principal_id`.** Its
  sibling `effective-permissions` adds inherited privileges but drops that
  field, which is the stable key the connector resolves principals by.
- **`max_results` must be `0` on the permissions endpoint.** Any value from 1 to
  149 is rejected with HTTP 400, because a page may not split one principal's
  privilege list.
- **A page token must never be synthesized.** The permissions endpoint silently
  ignores a token it cannot deserialize and restarts at page 1, so a synthesized
  token produces an endless re-read rather than an error. Only an absent token
  terminates a sequence: an empty page, and a page shorter than `max_results`,
  can both still carry one.
- **A metastore is addressed by its UUID.** Its name is rejected with HTTP 400.
- **A cached response is keyed to the host it came from.** The SDK's HTTP cache
  keys on the path, the query and a header set, with no host component, and every
  workspace deployment answers the catalog listing at the same path and query. The
  client adds a host header to the key so one workspace's listing is never served
  for another, which would file a workspace under a metastore it cannot reach.

## Permission model

Three independent layers have to line up, and granting one does not imply the
others:

1. **OAuth scopes** decide which API surfaces the credential may touch at all.
2. **The service principal's roles** decide what it sees on those surfaces.
3. **Unity Catalog privileges** apply on the securable itself.

The scopes:

| Scope | Covers |
|-------|--------|
| `unity-catalog` | Metastores, workspace metastore assignments, the catalog listing used to resolve access paths, and the permissions endpoint |
| `scim` | Account users, groups and service principals |
| `access-management` | The rule-sets API, where roles live, plus the workspace permission assignments |
| `provisioning` | The account workspaces listing. It is the only scope that grants it — including not the one named `workspace` |

The connector requests `all-apis`, which covers all four. Each resource type
declares its own scopes, roles and privileges, so `./baton-databricks
capabilities` reports them per type.

A privilege read is only trustworthy when the service principal satisfies at
least one of these per metastore:

- it is the owner of the metastore, or
- it is an admin of a workspace the metastore is assigned to.

Holding a privilege is not an option at this level: Databricks rejects `MANAGE`
and `ALL_PRIVILEGES` on a metastore, so there is no privilege whose presence
would prove the read is complete.

When neither holds, the connector fails the sync rather than reporting what
it can see. Databricks answers an under-permissioned privilege read with HTTP
200 and a partial list of privilege assignments, which is byte-indistinguishable
from an object that genuinely has no grants on it. Reporting that partial list
would make ConductorOne read every privilege the principal cannot see as revoked
access.

## Identity resolution

A privilege assignment carries only `principal` and `principal_id`, with no type
discriminator. `principal` holds a value from one of three namespaces: a user's
`userName`, a group's `displayName`, or a service principal's `applicationId`.

A SCIM filter built from that name cannot disambiguate it, because Databricks
answers an unmatched filter with HTTP 200 and zero rows rather than an error — a
miss is indistinguishable from a wrong-type lookup. The connector therefore
cross-references a per-sync index, keyed primarily on the numeric id.

The index is built from **account** SCIM, not workspace SCIM: workspace SCIM
sees only a fraction of the account's service principals and groups, and any of
the ones it cannot see can hold a Unity Catalog grant, because grants live on the
metastore rather than on a workspace.

A principal deleted in Databricks keeps its grants. Its assignment arrives with
an empty `principal` and only its numeric id, which is why the index resolves by
`principal_id` first. `principal_id` is accepted on removals only, so grant and
revoke build different request bodies.

## Limitations

- **Direct grants only.** Unity Catalog privileges inherit downward —
  `READ_METADATA` granted on a metastore inherits to every object beneath it —
  but the connector reports the privilege assignments recorded on the object
  itself. The endpoint that exposes inheritance does not return principal IDs,
  which are what the connector needs to resolve a principal unambiguously.
- **Ownership is read-only.** The owner of a metastore is shown but cannot be
  provisioned. An owner that is not an account identity — `System user`, or a
  workspace admins group such as `_workspace_admins_<workspace_id>` — does not
  resolve, and no owner grant is emitted for it.
- **`MANAGE` and `ALL_PRIVILEGES` do not exist at the metastore level** —
  Databricks rejects them there, so neither is offered as an entitlement.
- **A metastore needs a usable workspace.** Its privileges are not served on the
  account plane, so a metastore whose every attached workspace is excluded, not
  `RUNNING`, or unreadable fails the sync rather than being silently omitted.
- **Other securables are not synced yet:** catalogs, schemas, tables, volumes,
  functions, registered models, external locations, storage credentials,
  connections, shares and Delta Sharing recipients.
- **Tags are not synced**, including the `SYSTEM.TEAM` convention some customers
  use to drive just-in-time access.
- **Grants land after resources.** ConductorOne ingests grants a few minutes
  behind resources, so a resource can briefly show zero grants right after a
  sync.
