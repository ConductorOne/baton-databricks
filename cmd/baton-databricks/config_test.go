package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

const (
	flagSeparator = " "
	flagPrefix    = "--"
)

// parseFlags - convert command line flags to a map of keys to strings.
func parseFlags(expression string) (map[string]string, error) {
	output := make(map[string]string)
	tokens := strings.Split(expression, flagSeparator)
	currentFlag := ""
	for _, token := range tokens {
		if strings.HasPrefix(token, flagPrefix) {
			if currentFlag != "" {
				output[currentFlag] = ""
			}
			currentFlag = strings.TrimPrefix(token, flagPrefix)
		} else {
			if currentFlag == "" {
				return nil, fmt.Errorf("got a value without a flag: %s", token)
			}
			output[currentFlag] = token
			currentFlag = ""
		}
	}
	// Clean up final flag if it exists.
	if currentFlag != "" {
		output[currentFlag] = ""
	}

	return output, nil
}

func makeViper(input map[string]string) *viper.Viper {
	output := viper.New()
	for key, value := range input {
		output.Set(key, value)
	}
	return output
}

func assertOutcome(
	t *testing.T,
	function func() error,
	expectedSuccess bool,
) {
	err := function()
	if err != nil {
		if expectedSuccess {
			t.Fatal("expected function to succeed, but", err.Error())
		}
	} else {
		if !expectedSuccess {
			t.Fatal("expected function to fail, but it succeeded")
		}
	}
}

func TestConfigs(t *testing.T) {
	ctx := context.Background()

	configurationSchema := field.NewConfiguration(
		configurationFields,
		fieldRelationships...,
	)

	testCases := []struct {
		expression string
		isValid    bool
		message    string
	}{
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
	for _, testCase := range testCases {
		t.Run(testCase.message, func(t *testing.T) {
			values, err := parseFlags(testCase.expression)
			if err != nil {
				t.Fatal("could not parse flags:", err)
			}

			v := makeViper(values)

			assertOutcome(
				t,
				func() error {
					err = field.Validate(configurationSchema, v)
					if err != nil {
						return err
					}
					return validateConfig(ctx, v)
				},
				testCase.isValid,
			)
		})
	}
}
