While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.
   >
   > It also syncs Unity Catalog metastores, catalogs, schemas, tables, and volumes. Those resource types are opt-in — each stays off until enabled per resource type in C1.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   > - **Account lifecycle actions**: `enable_user` and `disable_user` set an account user's active status (account level only; permissions and memberships are kept).
   > - **Unity Catalog privileges**: grant and revoke a privilege on a metastore, catalog, schema, table, or volume for a user, group or service principal. The `owner` entitlement is read-only and cannot be granted or revoked.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID and a service principal OAuth client ID. It syncs the account and every workspace that service principal can access. The service principal is then authenticated in one of three ways:
   >
   > - **OAuth client secret** (default): the service principal's OAuth client secret.
   > - **Workload identity federation**: an externally issued JWT identifying the workload, such as a SPIFFE JWT-SVID or a Kubernetes projected service account token, read from a file. The connector exchanges it for a short-lived Databricks token, so no long-lived secret is stored. This requires a service principal federation policy in Databricks. It is available only when you run the connector yourself, on the command line or in service mode, and is selected with `--auth-method workload-identity-file`.
   > - **Workload identity federation (C1-hosted)**: C1 signs a short-lived JWT for the connector, and Databricks exchanges it for a short-lived token. No credential is stored. The hosted runtime selects this method; it is not available from the command line.
   > - **Catalog sync** uses these same credentials and adds no configuration fields.
   >
   > The authentication method selects the credential.
   >
   > Google Cloud Platform and Azure Databricks customers also provide their Databricks domain, such as `gcp.databricks.com` or `azuredatabricks.net`. Account Hostname is optional: the connector derives it from Hostname when unset.

2. For each item in the list above:
   - How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process.

     > - **Account ID**: in the Databricks account console, open the menu next to your username in the upper-right corner; the account ID is shown there.
     > - **OAuth client ID and secret**: follow the [Databricks OAuth (M2M) documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a service principal and generate an OAuth secret.
     > - **Workload identity federation**: follow the [Databricks OAuth token federation documentation](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-exchange) to add a federation policy to that same service principal. For a self-run connector, the JWT comes from the workload's own platform, not from Databricks.
     > - **Workload identity federation (C1-hosted)**: add a federation policy to that same service principal. Point it at the C1 OIDC issuer `<tenant endpoint>/auth/v1/workload-identity`, which publishes its own discovery document and JWKS. The audience is the Databricks OAuth client ID, and the subject is `c1:tenant/<tenant id>/app/<app id>/connector/<connector id>`.
     > - **Deployment name**: the subdomain in the workspace URL (not the numeric workspace ID).

   - Does the credential need any specific scopes or permissions? If so, list them here.

     > The credential must have admin access to each resource it reads or writes: account-admin on the Databricks account (for account-level sync and provisioning) and workspace-admin assigned directly to the service principal on each workspace being synced (Admin inherited only through a group is not enough).
     >
     > The OAuth token carries scopes on top of that. The connector requests `all-apis`, which covers the four it uses: `unity-catalog` (the securables and the permissions endpoint), `scim` (account principals), `access-management` (roles and workspace permission assignments) and `provisioning` (the account workspaces listing).
     >
     > Unity Catalog adds a third layer on top of scopes and roles: standing on the securable itself. For each metastore being synced, the service principal must either own the metastore or be assigned Admin directly on at least one running workspace attached to it. Without that standing the sync fails, and ConductorOne keeps the grants it already has.
     >
     > Catalog grants require metastore ownership, catalog ownership, effective `MANAGE` on the catalog, or direct Admin on the workspace whose `_workspace_admins_…` group owns the catalog. `ALL_PRIVILEGES` does not include `MANAGE`. A non-owner using `MANAGE` also needs `USE_CATALOG` for discovery.

   - If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate OAuth scopes. Catalog writes run the same trust check as reads, so they require metastore or catalog ownership, `MANAGE` on the catalog, or direct Admin on the workspace whose admins group owns it; proving a catalog absent requires metastore ownership.

   - What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Limitations

- **Direct grants only.** Databricks inherits privileges downward. The connector reports the assignments stored on the object. The inheritance endpoint does not return principal IDs.
- **Full grants visibility is required.** Metastore ownership or direct workspace Admin permits metastore reads. Catalog reads require one of the catalog permissions listed above; workspace Admin applies only when that workspace's admins group owns the catalog. Admin inherited through a group does not count. The sync fails instead of reporting a partial list, so C1 keeps the grants it already has.
- **Ownership is read-only.** It is shown and cannot be granted or revoked. An owner that is not an account identity (`System user`, `_workspace_admins_…`) is not shown.
- **Workspace admins discovery.** The group is synced only when metastores, groups and workspaces are all in the sync. A RUNNING workspace declares its `admins` group as a child, named `admins (<workspace name>)` because Databricks gives every workspace a group of that name and one metastore can serve several of them. Only members the account holds are emitted, because workspace-local identities are not synced. A privilege row whose workspace can still become RUNNING fails the sync, so the missing group is not read as revoked access; a row whose workspace is cancelling is skipped, because that workspace never becomes RUNNING.
- **Workspace admins privileges are read-only.** Metastore assignments to `_workspace_admins_<workspace_name>_<workspace_id>` resolve to that workspace's `admins` group, including its membership expansion, and the configured workspace allowlist and exclusions apply. The grants are immutable, and a grant or revoke that arrives anyway is refused with `FailedPrecondition` before any write: Databricks permits revoking these system grants but does not allow granting them back. The same refusal covers the group's membership. See [Workspace admin privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/admin-privileges#workspace-admin-privileges-when-workspaces-are-enabled-for-unity-catalog-automatically).
- **No `MANAGE` or `ALL_PRIVILEGES` on a metastore.** Databricks rejects both, so they are not offered.
- **Read through one running workspace.** Privileges are not served on the account plane. The connector reads each metastore through the first running workspace in scope where it can see every privilege. With no running workspace in scope, the metastore is not synced and ConductorOne removes it. A workspace that is still provisioning fails the sync until it is `RUNNING`. A workspace being deleted does the same, and the error names it and its status: pruning the metastore while the deletion runs would report it as gone and delete its grants, and the wait ends on its own when the account stops listing the workspace. An unreadable assignment list fails the sync and ConductorOne keeps the metastore.
- **`workspaces` scopes metastores.** An allowlist in which every name is either missing from the account or listed in `databricks-exclude-workspaces` fails at credential validation, before any sync, with one error that names both causes.
- **Catalogs are opt-in.** The resource type is off until it is turned on from the connector's configuration page in C1.
- **Catalog discovery visits every attached workspace in scope.** The connector asks each running workspace attached to the metastore, including for `ISOLATED` catalogs bound to only some workspaces. Each catalog name is kept once per metastore. A catalog bound only to workspaces the sync excludes is not listed, and C1 removes it with its grants.
- **A catalog is keyed by its name.** `CatalogInfo` exposes no immutable catalog id (only `name` and the derived `full_name`), so the resource id is `<metastore id>::<name>`. The update API accepts `new_name`, so an owner can rename a catalog; a rename re-keys the resource and its grants.
- **System catalogs are excluded regardless of ownership.** The built-in `system` catalog and any catalog with `CatalogType == "SYSTEM_CATALOG"` are excluded, including `samples` when it has that type.
- **Seeing a catalog needs more than `MANAGE`.** The list holds the catalogs the service principal owns or holds `USE_CATALOG` on, or every catalog when it is the metastore admin. `MANAGE` alone reads a catalog's privileges but does not make it appear.
- **Schemas are opt-in and hang from a catalog.** The resource type is off until it is turned on in C1, and it syncs nothing unless Catalogs and Metastores are on as well.
- **A schema is keyed by its full name.** `SchemaInfo` does carry an immutable `schema_id`, but no endpoint accepts it: `GET` and `PATCH` on a schema both address it as `catalog.schema`, so keying on the id would mean listing the catalog's schemas on every read and write just to recover that name. The parent catalog has no immutable id at all, so a catalog rename re-keys the schema either way. The resource id is `<metastore id>::<catalog>.<schema>`, and a schema rename re-keys the resource and its grants, as it does for a catalog.
- **`information_schema` is skipped in every catalog.** Databricks creates it read-only in every catalog and its privileges cannot be granted, so it would sync entitlements no one can hold or request.
- **Listing schemas is weaker than reading their privileges.** `USE_SCHEMA` and `ALL_PRIVILEGES` on the catalog each make the listing complete. Neither reads a schema's privileges, because Databricks leaves `MANAGE` out of `ALL_PRIVILEGES`: that read needs ownership of the metastore, the catalog or the schema, `MANAGE` on the schema or the catalog, or direct workspace Admin where the admins group owns the catalog.
- **Tables and volumes are opt-in and hang from a schema.** Each is off until it is turned on in C1, and syncs nothing unless Schemas, Catalogs and Metastores are on as well.
- **A table and a volume are keyed by their full name.** Neither exposes an immutable id, so the resource id extends the schema key: `<metastore id>::<catalog>.<schema>.<name>`. A rename re-keys the resource and its grants, as it does for a catalog and a schema.
- **One `table` securable covers more than tables.** Views, materialized views, metric views and streaming tables are all `table`. `table_type` picks the privilege set, so a table's entitlements are built per instance. `ALL_PRIVILEGES` on a view, `MODIFY` on a streaming table, and `REFRESH` on a type whose grants API refuses it are reported when already held and cannot be requested.
- **Listing a schema's tables or volumes is stricter than listing its schemas.** `USE_SCHEMA` and `ALL_PRIVILEGES` on the catalog each list every schema; only `ALL_PRIVILEGES` lists every table and volume, alongside ownership of the metastore, the catalog or the schema, or direct workspace Admin where the admins group owns the catalog. Reading one table's or one volume's privileges needs ownership of the metastore, the catalog, the schema or the object itself, or `MANAGE` on the object, its schema or its catalog.
- **Not synced yet:** functions, registered models, external locations, storage credentials, connections, shares, recipients, and tags.
