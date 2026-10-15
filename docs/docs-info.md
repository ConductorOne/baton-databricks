While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.
   >
   > It also syncs Unity Catalog metastores and catalogs. Both resource types are opt-in — each stays off until it is enabled per resource type in ConductorOne.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   > - **Unity Catalog privileges**: grant and revoke a metastore or catalog privilege for a user, group or service principal. The `owner` entitlement is read-only; granting or revoking it returns `InvalidArgument`.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID and OAuth credentials:
   >
   > - **OAuth**: a service principal OAuth client ID and client secret. Syncs the account and every workspace the service principal can access. This is the connector's only authentication method.
   >
   > Google Cloud Platform and Azure Databricks customers also provide the workspace hostname. Account Hostname is optional: the connector derives it from Hostname when unset.

2. For each item in the list above:

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process.

     > - **Account ID**: in the Databricks account console, open the menu next to your username in the upper-right corner; the account ID is shown there.
     > - **OAuth client ID and secret**: follow the [Databricks OAuth (M2M) documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a service principal and generate an OAuth secret.
     > - **Deployment name**: the subdomain in the workspace URL (not the numeric workspace ID).

   * Does the credential need any specific scopes or permissions? If so, list them here.

     > The credential must have admin access to each resource it reads or writes: account-admin on the Databricks account (for account-level sync and provisioning) and workspace-admin assigned directly to the service principal on each workspace being synced (Admin inherited only through a group is not enough).
     >
     > The OAuth token carries scopes on top of that. The connector requests `all-apis`, which covers the four it uses: `unity-catalog` (the securables and the permissions endpoint), `scim` (account principals), `access-management` (roles and workspace permission assignments) and `provisioning` (the account workspaces listing). The scope-by-scope breakdown is in `## Permission model` below.
     >
     > Unity Catalog adds a third layer on top of scopes and roles: standing on the securable itself. For each metastore being synced, the service principal must either own the metastore or be assigned Admin directly on a workspace it is assigned to. A privilege read cannot settle it at this level, because Databricks rejects `MANAGE` and `ALL_PRIVILEGES` on a metastore. A catalog is synced only when the service principal owns the metastore, owns the catalog, holds `MANAGE` on the catalog, or is a direct Admin of the workspace whose admins group owns it. A catalog that does not meet that is left out of the sync. `## Permission model` explains why.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate scopes: Databricks admin access covers both read (sync) and read-write (provision).

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Unity Catalog API

A metastore lives on the account plane, but every securable below it is reached
through a workspace host assigned to that metastore.

| Level | Endpoint |
|-------|----------|
| Metastores | `GET /api/2.0/accounts/{account_id}/metastores` |
| Workspace metastore assignment | `GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/metastore` |
| Catalogs | `GET /api/2.1/unity-catalog/catalogs` |
| Privileges on every level | `GET` and `PATCH` `/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}` |

- **HTTP cache keys include the request Host.** Two workspaces share paths such
  as `/unity-catalog/permissions/...`, and without the host in the key one
  workspace's response would be served for another.

## Permission model

Three independent layers have to line up, and granting one does not imply the
others:

1. **OAuth scopes** decide which API surfaces the credential may touch at all.
2. **The service principal's roles** decide what it sees on those surfaces.
3. **Unity Catalog privileges** apply on the securable itself.

The scopes:

| Scope | Covers |
|-------|--------|
| `unity-catalog` | Metastores, metastore workspace assignments, catalogs, and the permissions endpoint |
| `scim` | Account users, groups and service principals |
| `access-management` | The rule-sets API, where roles live, plus the workspace permission assignments |
| `provisioning` | The account workspaces listing. It is the only scope that grants it — including not the one named `workspace` |

The connector requests `all-apis`, which covers all four. Each resource type
declares its own scopes, roles and privileges, so `./baton-databricks
capabilities` reports them per type.

A privilege read is only trustworthy when the service principal satisfies at
least one of these per metastore:

- it is the owner of the metastore, or
- it is assigned Admin directly on a workspace the metastore is assigned to
  (Admin inherited only through a group is not enough).

Holding a privilege is not an option at this level: Databricks rejects `MANAGE`
and `ALL_PRIVILEGES` on a metastore, so there is no privilege whose presence
would prove the read is complete.

For a catalog, the read is trustworthy when the service principal owns the
metastore, owns the catalog, or holds `MANAGE` on it. It is also trustworthy
when the catalog owner is that workspace's admins group
(`_workspace_admins_<workspace>_<workspace id>`) and the service principal is
assigned Admin directly on the workspace. Workspace Admin does not open a
catalog owned by anyone else. `ALL_PRIVILEGES` does not count either: it
excludes `MANAGE`.

A catalog that meets none of these fails the catalog sync with
`PermissionDenied` naming the catalog, and ConductorOne does not remove it.
Databricks answers an under-permissioned privilege read with HTTP 200 and a
partial list of privilege assignments, which is byte-indistinguishable from an
object that genuinely has no grants on it. Reporting that partial list would
make ConductorOne revoke every privilege the principal cannot see. Leaving the
catalog out would delete it and its grants. A catalog that disappears between
the listing and this check (`CATALOG_DOES_NOT_EXIST`) is left off that page.
Any other permissions 404 fails the sync with `FailedPrecondition`: a
`NotFound` on the page would drop the other catalogs listed with it.

## Limitations

- **Direct grants only.** Unity Catalog privileges inherit downward —
  `READ_METADATA` granted on a metastore inherits to every object beneath it —
  but the connector reports the privilege assignments recorded on the object
  itself. The endpoint that exposes inheritance does not return principal IDs,
  which are what the connector needs to resolve a principal unambiguously.
- **Workspace Admin must be assigned directly.** The connector matches the
  service principal's application ID or its own SCIM id against the workspace's
  ADMIN assignments. Admin inherited only through a group is not treated as
  enough.
- **Ownership is read-only.** The owner of a metastore or catalog is shown but
  cannot be provisioned. An owner that is not an account identity — `System user`, or a
  workspace admins group such as `_workspace_admins_<workspace_id>` — does not
  resolve, and no owner grant is emitted for it.
- **`MANAGE` and `ALL_PRIVILEGES` do not exist at the metastore level** —
  Databricks rejects them there, so neither is offered as an entitlement.
- **A metastore needs a usable workspace.** Its privileges are not served on the
  account plane, only through a `RUNNING` workspace attached to it. A metastore
  with no workspace attached, or whose attached workspaces are all left out by
  `workspaces` or `databricks-exclude-workspaces`, is not synced, and
  ConductorOne removes it and its grants. Granting or revoking a privilege on
  such a metastore fails with `FailedPrecondition` and changes nothing in
  Databricks. An allowlist that matches no
  workspace on the account fails the sync. While an attached in-scope workspace
  is not `RUNNING`, or a metastore's workspace assignments cannot be read,
  nothing is removed: that metastore fails the sync instead.
- **Admin on any attached workspace is enough.** The connector tries each
  attached workspace in turn and reads through the first one where it holds
  Admin directly.
- **A catalog is synced when a workspace in the sync lists it.** An `ISOLATED`
  catalog is listed only by the workspaces bound to it, so one bound only to
  workspaces outside the sync is not synced. If a synced workspace cannot list
  catalogs, the catalog sync for that metastore fails rather than reporting a
  short list.
- **Revoking on a catalog no synced workspace lists** reports it already revoked
  only when the connector can prove it was deleted. That proof needs the
  service principal to own the metastore. Otherwise the revoke fails with
  `FailedPrecondition` and changes nothing.
- **Other securables are not synced yet:** schemas, tables, volumes,
  functions, registered models, external locations, storage credentials,
  connections, shares and Delta Sharing recipients.
- **Tags are not synced**, including the `SYSTEM.TEAM` convention some customers
  use to drive just-in-time access.
