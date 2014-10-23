package rdf

import (
	"testing"
)

func mustOpenDb(t *testing.T) *Database {
	db, err := OpenDatabase("graph", "graph", "graph", "localhost")
	if err != nil {
		t.Fatalf("error opening database: %v", err)
	}
	_ = db
	return db
}

func TestOpen(t *testing.T) {
	db := mustOpenDb(t)
	db.Close()
}

func TestInsertSomeData(t *testing.T) {
	db := mustOpenDb(t)
	defer db.Close()

	err := db.CreateResourceAlias("local")

	if err != nil {
		t.Fatalf("error creating table: %v", err)
	}
	if err := db.Truncate("local"); err != nil {
		t.Errorf("error truncating table: %v", err)
	}

	cs, err := db.Begin()
	if err != nil {
		t.Errorf("error starting changeset: %v", err)
	}

	node := &RdfNode{
		Res:     "local:123",
		Subject: "local:ContactInfo",
		Value: map[string]interface{}{
			"Phone":     "xxx-2123-3431",
			"Available": "morning",
		},
	}

	if err := cs.Save(node); err != nil {
		t.Errorf("error saving node: %v", err)
	}

	node = &RdfNode{
		Res:     "local:123",
		Subject: "local:Email",
		Type:    Ref,
		Value:   "local:person@example.org",
	}

	if err := cs.Save(node); err != nil {
		t.Errorf("error saving node (with ref value): %v", err)
	}

	if err := cs.Done(); err != nil {
		t.Errorf("error terminating changeset: %v", err)
	}

	if err := cs.Err(); err != nil {
		t.Errorf("first error on changeset: %v", err)
	}
}
