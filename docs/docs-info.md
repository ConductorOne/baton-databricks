While developing the connector, please fill out this form. This information is needed to write docs and to help other users set up the connector.

## Connector capabilities

1. What resources does the connector sync?

   > The connector syncs the Databricks account, workspaces, users, groups, service principals, and roles.

2. Can the connector provision any resources? If so, which ones?

   > Yes:
   >
   > - **User accounts**: create and delete account users.
   > - **Entitlements**: grant and revoke role and membership assignments on accounts, workspaces, groups, service principals, and roles.
   >
   > Provisioning of account groups is only available with OAuth. A workspace token cannot provision groups, because the Databricks API does not allow it from a workspace token.

## Connector credentials

1. What credentials or information are needed to set up the connector? (For example, API key, client ID and secret, domain, etc.)

   > The connector requires a Databricks account ID plus one of two authentication methods:
   >
   > - **OAuth (recommended)**: a service principal OAuth client ID and client secret. Syncs the account and every workspace the service principal can access.
   > - **Workspace token (PAT)**: one or more workspace personal access tokens paired positionally with the deployment names of the workspaces they authenticate. Scoped to the listed workspaces only.
   >
   > Google Cloud Platform and Azure Databricks customers also provide the account hostname and hostname.

2. For each item in the list above:

   * How does a user create or look up that credential or info? Please include links to (non-gated) documentation, screenshots (of the UI or of gated docs), or a video of the process.

     > - **Account ID**: in the Databricks account console, open the menu next to your username in the upper-right corner; the account ID is shown there.
     > - **OAuth client ID and secret**: follow the [Databricks OAuth (M2M) documentation](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html) to create a service principal and generate an OAuth secret.
     > - **Workspace token**: in the workspace, go to **Settings** > **Developer** > **Access tokens**, click **Manage**, then **Generate new token**.
     > - **Deployment name**: the subdomain in the workspace URL (not the numeric workspace ID).

   * Does the credential need any specific scopes or permissions? If so, list them here.

     > The credential must have admin access to each resource it reads or writes: account-admin on the Databricks account (for account-level sync and provisioning) and workspace-admin on each workspace being synced. Workspace-token auth only reaches the workspaces its tokens are scoped to.

   * If applicable: Is the list of scopes or permissions different to sync (read) versus provision (read-write)? If so, list the difference here.

     > No separate scopes: Databricks admin access covers both read (sync) and read-write (provision). The practical difference is coverage by auth method: OAuth reaches the account plane and all accessible workspaces; a workspace token reaches only its scoped workspaces and cannot read or write account-level entitlements, grants, or groups.

   * What level of access or permissions does the user need in order to create the credentials? (For example, must be a super administrator, must have access to the admin console, etc.)

     > Account admin access to the Databricks account console (to create the service principal and OAuth secret), and workspace admin on each workspace (to mint workspace tokens).
