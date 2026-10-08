While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.
   >
   > It also syncs the Unity Catalog securables it currently covers: metastores and catalogs. Both resource types are opt-in — each one stays off until it is enabled per resource type in ConductorOne.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   > - **Unity Catalog privileges**: grant and revoke a privilege for a user, group or service principal on both securable levels it covers (metastore, catalog). The `owner` entitlement is read-only; granting or revoking it returns `InvalidArgument`.

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
     > Unity Catalog adds a third layer on top of scopes and roles: privileges on the securable itself. For each catalog being synced, the service principal must satisfy at least one of — owner of the metastore, owner of the catalog, holder of `MANAGE` on the catalog, or admin of a workspace the catalog is reached through. `ALL_PRIVILEGES` does not include `MANAGE` or `READ METADATA`, so it does not make the privilege read complete. `## Permission model` explains why none of these is optional.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate scopes: Databricks admin access covers both read (sync) and read-write (provision).

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Unity Catalog API

A metastore lives on the account plane, but every securable below it is reached
through a workspace host assigned to that metastore. The connector enumerates
metastores from the account, resolves which workspaces are assigned to each one,
and issues the securable calls against one of those workspace hosts.

| Level | Endpoint |
|-------|----------|
| Metastores | `GET /api/2.0/accounts/{account_id}/metastores` |
| Workspace metastore assignment | `GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/metastore` |
| Catalogs | `GET /api/2.1/unity-catalog/catalogs` |
| Privileges on every level | `GET` and `PATCH` `/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` |

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
  for another, which would both hide an isolated catalog and answer one
  metastore's catalog with another's grants.

## Permission model

Three independent layers have to line up, and granting one does not imply the
others:

1. **OAuth scopes** decide which API surfaces the credential may touch at all.
2. **The service principal's roles** decide what it sees on those surfaces.
3. **Unity Catalog privileges** apply on the securable itself.

The scopes:

| Scope | Covers |
|-------|--------|
| `unity-catalog` | Metastores, workspace metastore assignments, catalogs, and the permissions endpoint |
| `scim` | Account users, groups and service principals |
| `access-management` | The rule-sets API, where roles live, plus the workspace permission assignments |
| `provisioning` | The account workspaces listing. It is the only scope that grants it — including not the one named `workspace` |

The connector requests `all-apis`, which covers all four. Each resource type
declares its own scopes, roles and privileges, so `./baton-databricks
capabilities` reports them per type.

A privilege read is only trustworthy when the service principal satisfies at
least one of these per catalog:

- it is the owner of the metastore,
- it is the owner of the catalog,
- it holds `MANAGE` on the catalog (`ALL_PRIVILEGES` does not include `MANAGE` or `READ METADATA`, so it does not make this read complete), or
- it is an admin of a workspace the catalog is reached through.

When none of them holds, the connector fails the sync rather than reporting what
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

- **Direct grants only.** Unity Catalog privileges inherit downward — `SELECT`
  on a catalog grants `SELECT` on every table under it — but the connector
  reports the privilege assignments recorded on each object. Inherited access
  shows on the ancestor that carries it and is not repeated on each descendant,
  so a catalog that inherits `READ_METADATA` from its metastore shows no holders
  of its own. The endpoint that exposes inheritance
  does not return principal IDs, which are what the connector needs to resolve a
  principal unambiguously.
- **Object-level privileges only.** Row filters, column masks and ABAC policies
  are not synced.
- **Ownership is read-only.** The owner of an object is shown but cannot be
  provisioned. An owner that is not an account identity — `System user`, or a
  workspace admins group such as `_workspace_admins_<workspace_id>` — does not
  resolve, and no owner grant is emitted for it.
- **`ALL_PRIVILEGES` is reported literally**, not expanded into the individual
  privileges it implies, because no API exposes that expansion.
- **`MANAGE` and `ALL_PRIVILEGES` do not exist at the metastore level** —
  Databricks rejects them there. `READ_METADATA` granted at the metastore
  inherits to every object beneath it.
- **Other securables are not synced yet:** schemas, tables, volumes, functions,
  registered models, external locations, storage credentials, connections, shares
  and Delta Sharing recipients.
- **Tags are not synced**, including the `SYSTEM.TEAM` convention some customers
  use to drive just-in-time access.
- **Isolated catalogs need a reachable workspace.** A catalog whose isolation
  mode is `ISOLATED` is only visible from the workspaces it is bound to, so the
  connector enumerates catalogs through every workspace it can reach and
  de-duplicates the result. A catalog bound only to a workspace the connector
  cannot reach fails the sync rather than being silently omitted.
- **The first sync attempts both types.** Because the types are opt-in,
  ConductorOne does not know they exist until the first sync reports them, so
  that sync covers both. On a large metastore that can be very long. Scope it with `--databricks-catalogs`, or restrict the run with
  `--sync-resource-types`, then opt in per type in ConductorOne afterwards.
- **Narrowing the catalog filter removes data.** Shrinking
  `--databricks-catalogs`, or widening `--databricks-exclude-catalogs`, after a
  successful sync removes the previously synced catalogs from ConductorOne, because they are legitimately absent from the new sync.
- **Grants land after resources.** ConductorOne ingests grants a few minutes
  behind resources, so a resource can briefly show zero grants right after a
  sync.
