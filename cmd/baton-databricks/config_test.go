package main

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/test"
	"github.com/conductorone/baton-sdk/pkg/ustrings"
	"github.com/spf13/viper"
)

func TestConfigs(t *testing.T) {
	ctx := context.Background()

	testCases := []test.TestCaseFromExpression{
		{
			"--account-id 1",
			false,
			"missing auth method",
		},
		{
			"--username 1 --password 1",
			false,
			"missing account-id",
		},
		{
			"--account-id 1 --username 1 --password 1",
			true,
			"username + password",
		},
		{
			"--account-id 1 --username 1",
			false,
			"missing password",
		},
		{
			"--account-id 1 --databricks-client-id 1 --databricks-client-secret 1",
			true,
			"client id + secret",
		},
		{
			"--account-id 1 --databricks-client-id 1",
			false,
			"missing client secret",
		},
		{
			"--account-id 1 --workspaces 1",
			false,
			"missing auth method, but has workspaces",
		},
		{
			"--account-id 1 --workspaces 1 --username 1 --password 1",
			true,
			"workspaces + username + password",
		},
		{
			"--account-id 1 --workspaces 1 --workspace-tokens 1",
			true,
			"auth tokens",
		},
		{
			"--account-id 1 --workspace-tokens 1",
			false,
			"mission workspaces",
		},
		{
			// Note this uses a _custom_ validation!
			"--account-id 1 --workspaces 1,2 --workspace-tokens 1",
			false,
			"not enough tokens",
		},
	}

	test.ExerciseTestCasesFromExpressions(
		t,
		field.NewConfiguration(
			configurationFields,
			fieldRelationships...,
		),
		func(configs *viper.Viper) error {
			return validateConfig(ctx, configs)
		},
		ustrings.ParseFlags,
		testCases,
	)
}
