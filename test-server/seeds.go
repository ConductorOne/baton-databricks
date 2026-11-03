package main

import "time"

// Seed IDs. DECISION: all made up; numeric IDs are 16 digits like real Databricks IDs.
const (
	wsAlphaID int64 = 7474645012345671
	wsBetaID  int64 = 7474645012345672
	wsGammaID int64 = 8828000011112223

	userAlice = "4410000000000001"
	userBob   = "4410000000000002"
	userCarol = "4410000000000003"
	userDave  = "4410000000000004"

	groupDataEngineers = "5520000000000001"
	groupAnalysts      = "5520000000000002"

	spCIBot = "6630000000000001"

	// deletedPrincipal is named in an audit row but matches no user, group or service principal.
	deletedPrincipal = "9990000000000001"
)

// seedState builds the in-memory account. Audit row times are relative to start.
func seedState(start time.Time) *state {
	st := &state{
		// DOC: deployment_name is the workspace hostname prefix; aws_region per workspace.
		workspaces: []workspace{
			{ID: wsAlphaID, Name: "Alpha", DeploymentName: "ws-alpha", Region: "us-east-1"},
			{ID: wsBetaID, Name: "Beta", DeploymentName: "ws-beta", Region: "us-east-1"},
			{ID: wsGammaID, Name: "Gamma", DeploymentName: "ws-gamma", Region: "eu-west-1"},
		},
		users: map[string]scimUser{
			userAlice: {ID: userAlice, UserName: "alice@example.com", DisplayName: "Alice Anders", Active: true},
			userBob:   {ID: userBob, UserName: "bob@example.com", DisplayName: "Bob Brown", Active: true},
			userCarol: {ID: userCarol, UserName: "carol@example.com", DisplayName: "Carol Chen", Active: false},
			userDave:  {ID: userDave, UserName: "dave@example.com", DisplayName: "Dave Diaz", Active: true},
		},
		groups: map[string]scimGroup{
			groupDataEngineers: {ID: groupDataEngineers, DisplayName: "data-engineers"},
			groupAnalysts:      {ID: groupAnalysts, DisplayName: "analysts"},
		},
		servicePrincipals: map[string]scimServicePrincipal{
			spCIBot: {ID: spCIBot, ApplicationID: "0d1c2b3a-0000-4000-8000-000000000001", DisplayName: "ci-bot", Active: true},
		},
		// DECISION: one running serverless warehouse, one stopped pro warehouse, one that never starts.
		warehouses: map[string]*warehouse{
			"wh-serverless": {
				ID: "wh-serverless", Name: "Serverless Starter Warehouse", DeploymentName: "ws-alpha",
				WarehouseType: "PRO", Serverless: true, InitialState: warehouseRunning,
			},
			"wh-pro": {
				ID: "wh-pro", Name: "Pro Warehouse", DeploymentName: "ws-beta",
				WarehouseType: "PRO", InitialState: warehouseStopped,
			},
			"wh-broken": {
				ID: "wh-broken", Name: "Broken Warehouse", DeploymentName: "ws-alpha",
				WarehouseType: "PRO", InitialState: warehouseStopped, NeverStarts: true,
			},
		},
		statements: map[string]*statement{},
	}
	st.audit = seedAudit(start)
	return st
}

// seedAudit returns synthetic system.access.audit rows.
// DOC: request_params keys per action follow https://docs.databricks.com/aws/en/admin/account-settings/audit-logs,
// and account-level events have workspace_id 0.
// DECISION: IDs and times are made up. Rows the connector should read fall between 26 and 5
// hours before start: older than its 4-hour lag window and inside an --event-feed-start-at
// of 2 days ago. Two extra rows sit outside that window on purpose.
func seedAudit(start time.Time) []auditRow {
	at := func(h, m int) time.Time {
		return start.Add(-time.Duration(h)*time.Hour - time.Duration(m)*time.Minute).UTC().Truncate(time.Millisecond)
	}
	user := func(endpoint, id, name string) map[string]string {
		return map[string]string{"endpoint": endpoint, "targetUserId": id, "targetUserName": name}
	}
	membership := func(groupID, groupName, memberID, memberName string) map[string]string {
		return map[string]string{
			"endpoint": "scim", "targetGroupId": groupID, "targetGroupName": groupName,
			"targetUserId": memberID, "targetUserName": memberName,
		}
	}
	tie := at(6, 0)

	return []auditRow{
		{EventID: "e0000001-0000-4000-8000-000000000001", EventTime: at(26, 0), ServiceName: "accounts", ActionName: "add",
			RequestParams: user("scim", userDave, "dave@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000002", EventTime: at(25, 0), ServiceName: "accounts", ActionName: "activateUser",
			RequestParams: user("scim", userBob, "bob@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000003", EventTime: at(24, 0), ServiceName: "accounts", ActionName: "deactivateUser",
			RequestParams: user("scim", userCarol, "carol@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000004", EventTime: at(23, 0), ServiceName: "accounts", ActionName: "createGroup",
			RequestParams: map[string]string{"endpoint": "scim", "targetGroupId": groupAnalysts, "targetGroupName": "analysts"}},
		{EventID: "e0000001-0000-4000-8000-000000000005", EventTime: at(22, 0), ServiceName: "accounts", ActionName: "addPrincipalToGroup",
			RequestParams: membership(groupDataEngineers, "data-engineers", userAlice, "alice@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000006", EventTime: at(21, 0), ServiceName: "accounts", ActionName: "addPrincipalToGroup",
			RequestParams: membership(groupDataEngineers, "data-engineers", groupAnalysts, "analysts")},
		{EventID: "e0000001-0000-4000-8000-000000000007", EventTime: at(20, 0), ServiceName: "accounts", ActionName: "addPrincipalsToGroup",
			RequestParams: membership(groupAnalysts, "analysts", spCIBot, "0d1c2b3a-0000-4000-8000-000000000001")},
		{EventID: "e0000001-0000-4000-8000-000000000008", EventTime: at(19, 0), ServiceName: "accounts", ActionName: "removePrincipalFromGroup",
			RequestParams: membership(groupDataEngineers, "data-engineers", userBob, "bob@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000009", EventTime: at(18, 0), ServiceName: "accounts", ActionName: "removePrincipalFromGroup",
			RequestParams: membership(groupDataEngineers, "data-engineers", deletedPrincipal, "gone@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000010", EventTime: at(17, 0), ServiceName: "accounts", ActionName: "setAccountAdmin",
			RequestParams: user("adminConsole", userAlice, "alice@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000011", EventTime: at(16, 0), WorkspaceID: wsAlphaID, ServiceName: "accounts", ActionName: "setAdmin",
			RequestParams: user("adminConsole", userBob, "bob@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000012", EventTime: at(15, 0), WorkspaceID: wsAlphaID, ServiceName: "accounts", ActionName: "removeAdmin",
			RequestParams: user("adminConsole", userBob, "bob@example.com")},
		// DOC: workspace-level createGroup has no targetGroupId.
		{EventID: "e0000001-0000-4000-8000-000000000013", EventTime: at(14, 0), WorkspaceID: wsAlphaID, ServiceName: "accounts", ActionName: "createGroup",
			RequestParams: user("scim", userAlice, "alice@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000014", EventTime: at(13, 0), WorkspaceID: wsBetaID, ServiceName: "accounts", ActionName: "delete",
			RequestParams: user("scim", userCarol, "carol@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000015", EventTime: at(12, 0), ServiceName: "workspace", ActionName: "addPermissionAssignment",
			RequestParams: map[string]string{"principal_id": userDave, "account_id": testAccountID, "workspace_id": "7474645012345671"}},
		// DOC: updatePermissionAssignment has no workspace_id.
		{EventID: "e0000001-0000-4000-8000-000000000016", EventTime: at(11, 0), ServiceName: "workspace", ActionName: "updatePermissionAssignment",
			RequestParams: map[string]string{"principal_id": userDave, "permissions": `["ADMIN"]`}},
		{EventID: "e0000001-0000-4000-8000-000000000017", EventTime: at(10, 0), WorkspaceID: wsBetaID, ServiceName: "workspace", ActionName: "deletePermissionAssignment",
			RequestParams: map[string]string{"principal_id": userBob, "account_id": testAccountID, "workspace_id": "7474645012345672"}},
		// ws-gamma is in eu-west-1, so this row is invisible from a us-east-1 warehouse.
		{EventID: "e0000001-0000-4000-8000-000000000018", EventTime: at(9, 0), WorkspaceID: wsGammaID, ServiceName: "accounts", ActionName: "add",
			RequestParams: user("scim", userAlice, "alice@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000019", EventTime: at(8, 0), WorkspaceID: wsAlphaID, ServiceName: "accounts", ActionName: "updateUser",
			RequestParams: user("scim", userAlice, "alice@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000020", EventTime: at(7, 0), ServiceName: "accounts", ActionName: "updateGroup",
			RequestParams: map[string]string{"endpoint": "scim", "targetGroupId": groupAnalysts, "targetGroupName": "analysts"}},
		// Two rows with the same event_time, to exercise the (event_time, event_id) ordering.
		{EventID: "e0000001-0000-4000-8000-000000000021", EventTime: tie, ServiceName: "accounts", ActionName: "removeGroup",
			RequestParams: map[string]string{"endpoint": "scim", "targetGroupId": groupDataEngineers, "targetGroupName": "data-engineers"}},
		{EventID: "e0000001-0000-4000-8000-000000000022", EventTime: tie, ServiceName: "accounts", ActionName: "deactivateUser",
			RequestParams: user("scim", userDave, "dave@example.com")},
		{EventID: "e0000001-0000-4000-8000-000000000023", EventTime: at(5, 30), WorkspaceID: wsAlphaID, ServiceName: "accounts", ActionName: "changeDatabricksWorkspaceAcl",
			RequestParams: map[string]string{"shardName": "ws-alpha", "targetUserId": userCarol, "resourceId": "7474645012345671", "aclPermissionSet": "workspace-access"}},
		// Not an action the connector asks for.
		{EventID: "e0000001-0000-4000-8000-000000000024", EventTime: at(5, 0), WorkspaceID: wsAlphaID, ServiceName: "clusters", ActionName: "create",
			RequestParams: map[string]string{"cluster_name": "etl"}},
		// Outside the window: newer than the connector's 4-hour lag cutoff.
		{EventID: "e0000001-0000-4000-8000-000000000025", EventTime: at(2, 0), ServiceName: "accounts", ActionName: "add",
			RequestParams: user("scim", userBob, "bob@example.com")},
		// Outside the window: older than an --event-feed-start-at of 2 days ago.
		{EventID: "e0000001-0000-4000-8000-000000000026", EventTime: at(72, 0), ServiceName: "accounts", ActionName: "add",
			RequestParams: user("scim", userCarol, "carol@example.com")},
	}
}
