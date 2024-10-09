package config

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
	}

	configurations := field.NewConfiguration(
		configurationFields,
		fieldRelationships...,
	)

	extraValidationFunction := func(configs *viper.Viper) error {
		return ValidateConfig(ctx, configs)
	}

	test.ExerciseTestCasesFromExpressions(
		t,
		configurations,
		extraValidationFunction,
		ustrings.ParseFlags,
		testCases,
	)

	t.Run("should validate token list lengths match", func(t *testing.T) {
		v := viper.New()
		v.Set("account-id", "1")
		v.Set("workspaces", []string{"1", "2"})

		f := func() error {
			err := field.Validate(configurations, v)
			if err != nil {
				return err
			}
			return extraValidationFunction(v)
		}

		t.Run("should fail", func(t *testing.T) {
			v.Set("workspace-tokens", []string{"3"})

			test.AssertValidation(t, f, false)
		})

		t.Run("should succeed", func(t *testing.T) {
			v.Set("workspace-tokens", []string{"3", "4"})

			test.AssertValidation(t, f, true)
		})
	})
}
