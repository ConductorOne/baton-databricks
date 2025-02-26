#!/bin/bash

set -exo pipefail

if [ -z "$BATON_DATABRICKS" ]; then
  echo "BATON_DATABRICKS not set. using baton-databricks"
  BATON_DATABRICKS=baton-databricks
fi
if [ -z "$BATON" ]; then
  echo "BATON not set. using baton"
  BATON=baton
fi

# Error on unbound variables now that we've set BATON & BATON_DATABRICKS
set -u

# Sync
$BATON_DATABRICKS

# Grant entitlement
$BATON_DATABRICKS --grant-entitlement="$CONNECTOR_ENTITLEMENT" --grant-principal="$CONNECTOR_PRINCIPAL" --grant-principal-type="$CONNECTOR_PRINCIPAL_TYPE"

# Check for grant before revoking
$BATON_DATABRICKS
$BATON grants --entitlement="$CONNECTOR_ENTITLEMENT" --output-format=json | jq --exit-status ".grants[] | select( .principal.id.resource == \"$CONNECTOR_PRINCIPAL\" )"

# Grant already-granted entitlement
$BATON_DATABRICKS --grant-entitlement="$CONNECTOR_ENTITLEMENT" --grant-principal="$CONNECTOR_PRINCIPAL" --grant-principal-type="$CONNECTOR_PRINCIPAL_TYPE"

# Get grant ID
CONNECTOR_GRANT=$($BATON grants --entitlement="$CONNECTOR_ENTITLEMENT" --output-format=json | jq --raw-output --exit-status ".grants[] | select( .principal.id.resource == \"$CONNECTOR_PRINCIPAL\" ).grant.id")

# Revoke grant
$BATON_DATABRICKS --revoke-grant="$CONNECTOR_GRANT"

# Revoke already-revoked grant
$BATON_DATABRICKS --revoke-grant="$CONNECTOR_GRANT"

# Check grant was revoked
$BATON_DATABRICKS
$BATON grants --entitlement="$CONNECTOR_ENTITLEMENT" --output-format=json | jq --exit-status "if .grants then [ .grants[] | select( .principal.id.resource == \"$CONNECTOR_PRINCIPAL\" ) ] | length == 0 else . end"

# Re-grant entitlement
$BATON_DATABRICKS --grant-entitlement="$CONNECTOR_ENTITLEMENT" --grant-principal="$CONNECTOR_PRINCIPAL" --grant-principal-type="$CONNECTOR_PRINCIPAL_TYPE"

# Check grant was re-granted
$BATON_DATABRICKS
$BATON grants --entitlement="$CONNECTOR_ENTITLEMENT" --output-format=json | jq --exit-status ".grants[] | select( .principal.id.resource == \"$CONNECTOR_PRINCIPAL\" )"
