package databricks

import (
	"errors"
	"testing"

	"golang.org/x/oauth2"
)

type failingTokenSource struct{}

func (failingTokenSource) Token() (*oauth2.Token, error) { return nil, errors.New("boom") }

func TestOAuth2TokenSubjectTokenSource(t *testing.T) {
	t.Parallel()

	got, err := OAuth2TokenSubjectTokenSource{Source: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "c1-jwt"})}.SubjectToken()
	if err != nil || got != "c1-jwt" {
		t.Fatalf("SubjectToken() = %q, %v; want c1-jwt", got, err)
	}

	if _, err := (OAuth2TokenSubjectTokenSource{Source: oauth2.StaticTokenSource(&oauth2.Token{})}).SubjectToken(); err == nil {
		t.Error("an empty token must be refused")
	}

	if _, err := (OAuth2TokenSubjectTokenSource{Source: failingTokenSource{}}).SubjectToken(); err == nil {
		t.Error("a token source error must be returned")
	}
}
