While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.
   >
   > It also syncs Unity Catalog metastores and their catalogs. Both resource types are opt-in — each stays off until enabled per resource type in ConductorOne.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   > - **Unity Catalog privileges**: grant and revoke a metastore or catalog privilege for a user, group or service principal. The `owner` entitlement is read-only and cannot be granted or revoked.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID and OAuth credentials:
   >
   > - **OAuth**: a service principal OAuth client ID and client secret. Syncs the account and every workspace the service principal can access. This is the connector's only authentication method.
   > - **Catalog sync** uses these same credentials and adds no configuration fields.
   >
   > Google Cloud Platform and Azure Databricks customers also provide their Databricks domain, such as `gcp.databricks.com` or `azuredatabricks.net`. Account Hostname is optional: the connector derives it from Hostname when unset.

2. For each item in the list above:

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process.

     > - **Account ID**: in the Databricks account console, open the menu next to your username in the upper-right corner; the account ID is shown there.
     > - **OAuth client ID and secret**: follow the [Databricks OAuth (M2M) documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a service principal and generate an OAuth secret.
     > - **Deployment name**: the subdomain in the workspace URL (not the numeric workspace ID).

   * Does the credential need any specific scopes or permissions? If so, list them here.

     > The credential must have admin access to each resource it reads or writes: account-admin on the Databricks account (for account-level sync and provisioning) and workspace-admin assigned directly to the service principal on each workspace being synced (Admin inherited only through a group is not enough).
     >
     > The OAuth token carries scopes on top of that. The connector requests `all-apis`, which covers the four it uses: `unity-catalog` (the securables and the permissions endpoint), `scim` (account principals), `access-management` (roles and workspace permission assignments) and `provisioning` (the account workspaces listing).
     >
     > Unity Catalog adds a third layer on top of scopes and roles: standing on the securable itself. For each metastore being synced, the service principal must either own the metastore or be assigned Admin directly on at least one running workspace attached to it. Without that standing the sync fails, and ConductorOne keeps the grants it already has.
     >
     > Catalog grants require metastore ownership, catalog ownership, effective `MANAGE` on the catalog, or direct Admin on the workspace whose `_workspace_admins_…` group owns the catalog. `ALL_PRIVILEGES` does not include `MANAGE`. A non-owner using `MANAGE` also needs `USE_CATALOG` for discovery.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate OAuth scopes. Catalog writes require metastore or catalog ownership, or `MANAGE` on the catalog; proving a catalog absent requires metastore ownership.

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Limitations

- **Direct grants only.** Databricks inherits privileges downward. The connector reports the assignments stored on the object. The inheritance endpoint does not return principal IDs.
- **Full grants visibility is required.** Metastore ownership or direct workspace Admin permits metastore reads. Catalog reads require one of the catalog permissions listed above; workspace Admin applies only when that workspace's admins group owns the catalog. Admin inherited through a group does not count. The sync fails instead of reporting a partial list, so ConductorOne keeps the grants it already has.
- **Ownership is read-only.** It is shown and cannot be granted or revoked. An owner that is not an account identity (`System user`, `_workspace_admins_…`) is not shown.
- **No `MANAGE` or `ALL_PRIVILEGES` on a metastore.** Databricks rejects both, so they are not offered.
- **Read through one running workspace.** Privileges are not served on the account plane. The connector reads each metastore through the first running workspace in scope where it can see every privilege. With no running workspace in scope, the metastore is not synced and ConductorOne removes it. A workspace that is still provisioning fails the sync until it is `RUNNING`. An unreadable assignment list fails the sync and ConductorOne keeps the metastore.
- **`workspaces` scopes metastores.** An allowlist that names no workspace on the account fails at credential validation, before any sync. A name that is also in `databricks-exclude-workspaces` is reported separately from a name the account does not have.
- **Catalogs are opt-in.** The resource type is off until it is turned on from the connector's configuration page in C1.
- **Catalog discovery visits every attached workspace in scope.** The connector asks each running workspace attached to the metastore, including for `ISOLATED` catalogs bound to only some workspaces. Each catalog name is kept once per metastore. A catalog bound only to workspaces the sync excludes is not listed, and ConductorOne removes it with its grants.
- **System catalogs are excluded regardless of ownership.** The built-in `system` catalog and any catalog with `CatalogType == "SYSTEM_CATALOG"` are excluded, including `samples` when it has that type.
- **Seeing a catalog needs more than `MANAGE`.** The list holds the catalogs the service principal owns or holds `USE_CATALOG` on, or every catalog when it is the metastore admin. `MANAGE` alone reads a catalog's privileges but does not make it appear.
- **Not synced yet:** tables, volumes, functions, registered models, external locations, storage credentials, connections, shares, recipients, and tags.
