package rdfc

import (
	"testing"
)

func mustOpenSession(t *testing.T) *Session {
	s := Session{}
	err := s.Open("graph", "graph", "graph", "localhost")
	if err != nil {
		t.Fatalf("error starting database: %v", err)
	}
	return &s
}

func TestSession(t *testing.T) {
	s := mustOpenSession(t)
	defer s.Close()

	// ensure that we don't have a user:Bob123 on the database
	s.Purge("user:Bob123")
	s.Done()

	if err := s.Set("user:Bob123", "user:Email", "bob123@example.com"); err != nil {
		t.Fatalf("error changing the user email: %v", err)
	}

	res, err := s.LoadResource("user:Bob123")
	if err != nil {
		t.Fatalf("error loading resource: %v", err)
	}

	if res.Get("user:Email").Str() != "bob123@example.com" {
		t.Fatalf("Unexpected email. should be %v got %v", "bob123@example.com", res.Get("user:Email").Str())
	}
}
