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
   > - **Unity Catalog privileges**: grant and revoke a metastore privilege for a user, group or service principal. The `owner` entitlement is read-only and cannot be granted or revoked.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID and OAuth credentials:
   >
   > - **OAuth**: a service principal OAuth client ID and client secret. Syncs the account and every workspace the service principal can access. This is the connector's only authentication method.
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
     > Unity Catalog adds a third layer on top of scopes and roles: standing on the securable itself. For each metastore being synced, the service principal must either own the metastore or be assigned Admin directly on a workspace it is assigned to. Without that standing Databricks answers the privilege read with the subset the caller may see, and ConductorOne reads the rest as revoked.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate scopes: Databricks admin access covers both read (sync) and read-write (provision).

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace being synced.

## Limitations

- **Direct grants only.** Unity Catalog privileges inherit downward —
  `READ_METADATA` granted on a metastore inherits to every object beneath it —
  but the connector reports the privilege assignments recorded on the object
  itself. The endpoint that exposes inheritance does not return principal IDs,
  which are what the connector needs to resolve a principal unambiguously.
- **Workspace Admin must be assigned directly.** Admin inherited only through a
  group does not let Databricks return every privilege assignment on a
  metastore, so the sync would report the subset the service principal can see
  as the complete set.
- **Ownership is read-only.** The owner of a metastore is shown but cannot be
  provisioned. An owner that is not an account identity — `System user`, or a
  workspace admins group such as `_workspace_admins_<workspace_id>` — does not
  resolve, and no owner grant is emitted for it.
- **`MANAGE` and `ALL_PRIVILEGES` do not exist at the metastore level** —
  Databricks rejects them there, so neither is offered as an entitlement.
- **A metastore needs a running workspace.** Its privileges are not served on
  the account plane, only through a running workspace attached to it. A
  metastore that no running workspace in the sync reaches is not synced, and
  ConductorOne removes it and its grants. Granting or revoking a privilege on
  such a metastore fails and changes nothing in Databricks.
- **Narrowing `workspaces` narrows the metastores.** Only a metastore attached
  to a listed workspace is synced. A list that names no workspace on the account
  is rejected when the connector validates its credentials, before any sync
  runs.
- **The workspace shown on a metastore is the access path.** `List` records the
  first workspace in the sync that the metastore is assigned to, and every
  privilege read, grant and revoke goes through it. Admin on that workspace is
  what the service principal needs.
- **Other securables are not synced yet:** functions, registered models,
  external locations, storage credentials, connections, shares and Delta Sharing
  recipients.
- **Tags are not synced**, including the `SYSTEM.TEAM` convention some customers
  use to drive just-in-time access.
