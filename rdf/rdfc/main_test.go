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

	if err := s.SetMany("user:Bob123", Node{S: "user:Email", V: NewString("bob123@example.com")}); err != nil {
		t.Fatalf("error changing the user email: %v", err)
	}

	res, err := s.LoadResource("user:Bob123")
	if err != nil {
		t.Fatalf("error loading resource: %v", err)
	}

	if res.Get("user:Email").Str() != "bob123@example.com" {
		t.Fatalf("Unexpected email. should be %v got %v", "bob123@example.com", res.Get("user:Email").Str())
	}
	s.Done()

	s2 := mustOpenSession(t)

	if res, err := s2.FindResource(Filter{S: "user:Email", V: NewString("bob123@example.com")}); err != nil {
		t.Errorf("error searching for resources... %v", err)
	} else {
		if len(res) != 1 {
			t.Errorf("should have found only one match. but got: %v", len(res))
		}

		if res[0].Id() != "user:Bob123" {
			t.Errorf("Invalid Id should be %v got %v", "user:Bob123", res[0].Id())
		}
	}
}
