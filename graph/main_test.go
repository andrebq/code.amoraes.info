package graph

import (
	"testing"
)

func mustOpenGraph(t *testing.T) *Graph {
	g := &Graph{}
	if err := g.Open("graph", "graph", "graph", "localhost"); err != nil {
		t.Fatalf("error opening graph database: %v", err)
	}
	return g
}

func TestSimpleGraph(t *testing.T) {
	g := mustOpenGraph(t)
	defer g.Close()

	user := User{
		Email: "bob@email.com",
	}

	perm := Permission{
		Desc: "can start a shell",
		Cmd:  "bash",
	}

	err := g.Save(&user)
	if err != nil {
		t.Fatalf("error saving user: %v", err)
	}

	err = g.Save(&perm)
	if err != nil {
		t.Fatalf("error saving permission: %v", err)
	}

	access := Access{
		User:       user.Id,
		Permission: perm.Id,
	}

	err = g.Connect(&access)
	if err != nil {
		t.Fatalf("error saving access relation: %v", err)
	}
}
