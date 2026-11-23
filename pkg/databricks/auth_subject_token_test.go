package databricks

import (
	"errors"
	"testing"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type funcTokenSource func() (*oauth2.Token, error)

func (f funcTokenSource) Token() (*oauth2.Token, error) { return f() }

func TestOAuth2TokenSubjectTokenSourceStatusCodes(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.FailedPrecondition, codes.Internal, codes.DeadlineExceeded, codes.Unauthenticated} {
		src := funcTokenSource(func() (*oauth2.Token, error) { return nil, status.Error(code, "c1") })
		_, err := OAuth2TokenSubjectTokenSource{Source: src}.SubjectToken()
		if got := status.Code(err); got != code {
			t.Errorf("code %v was relabelled %v", code, got)
		}
	}

	_, err := (OAuth2TokenSubjectTokenSource{Source: failingTokenSource{}}).SubjectToken()
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("unclassified error got %v, want FailedPrecondition", got)
	}

	nilSrc := funcTokenSource(func() (*oauth2.Token, error) { return nil, nil })
	if _, err := (OAuth2TokenSubjectTokenSource{Source: nilSrc}).SubjectToken(); err == nil {
		t.Error("a nil token must be refused, not panic")
	}
}
