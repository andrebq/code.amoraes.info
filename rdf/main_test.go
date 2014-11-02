package rdf

import (
	"testing"
)

func mustOpenDb(t *testing.T) *Database {
	db, err := OpenDatabase("graph", "graph", "graph", "localhost")
	if err != nil {
		t.Fatalf("error opening database: %v", err)
	}

	err = db.TruncateDatabase()
	if err != nil {
		t.Fatalf("error truncating database: %v", err)
	}
	_ = db
	return db
}

func purgeResource(t *testing.T, res string, db *Database) {
	cs, err := db.Begin()
	if err != nil {
		t.Fatalf("Error starting transaction: %v", err)
	}
	err = cs.Purge(res)
	if err != nil {
		t.Fatalf("error purging resource: %v", err)
	}
	cs.Done()
}

func TestOpen(t *testing.T) {
	db := mustOpenDb(t)
	db.Close()
}

func TestInsertSomeData(t *testing.T) {
	db := mustOpenDb(t)
	defer db.Close()
	var err error

	if err != nil {
		t.Fatalf("error creating table: %v", err)
	}
	purgeResource(t, "local:123", db)
	cs, err := db.Begin()
	if err != nil {
		t.Errorf("error starting changeset: %v", err)
	}

	node := &Node{
		Res:     "local:123",
		Subject: "local:ContactInfo",
		Value: map[string]interface{}{
			"Phone":     "xxx-2123-3431",
			"Available": "morning",
		},
	}

	if _, err := cs.Save(node); err != nil {
		t.Errorf("error saving node: %v", err)
	}

	node = &Node{
		Res:     "local:123",
		Subject: "local:Email",
		Type:    Ref,
		Value:   "local:person@example.org",
	}

	if _, err := cs.Save(node); err != nil {
		t.Errorf("error saving node (with ref value): %v", err)
	}

	if err := cs.Done(); err != nil {
		t.Errorf("error terminating changeset: %v", err)
	}

	if err := cs.Err(); err != nil {
		t.Errorf("first error on changeset: %v", err)
	}

	query := db.NewQuery()
	if err := query.FetchResource("local:123"); err != nil {
		t.Errorf("error fetching resource: %v", err)
	}

	if len(query.Result()) == 0 {
		t.Errorf("should have at least one result")
	}

	for _, v := range query.Result() {
		if v.Type == Doc {
			tmp := make(map[string]interface{})
			if err := v.ScanDocument(&tmp); err != nil {
				t.Errorf("error decoding subject %v: %v", v.Subject, err)
			}
		}
	}
	if err := query.Done(); err != nil {
		t.Errorf("error closing query: %v", err)
	}

	query = db.NewQuery()
	query.AddFilter(Filter{Subject: "local:Email", Type: Ref, Value: "local:person@example.org"})

	if err := query.Exec(); err != nil {
		t.Errorf("error running query: %v", err)
	}
	if len(query.Result()) == 0 {
		t.Errorf("should have at least one result")
	}

	for _, v := range query.Result() {
		if v.Type == Doc {
			tmp := make(map[string]interface{})
			if err := v.ScanDocument(&tmp); err != nil {
				t.Errorf("error decoding subject %v: %v", v.Subject, err)
			}
		}
	}
	if err := query.Done(); err != nil {
		t.Errorf("error closing query: %v", err)
	}

	// now let's purge and see if we still have the data in there
	if cs, err = db.Begin(); err != nil {
		t.Errorf("error starting transaction: %v", err)
	}
	if err = cs.Purge("local:123"); err != nil {
		t.Errorf("error purging resource: %v", err)
	}
	if err = cs.Done(); err != nil {
		t.Errorf("error finishing transaction: %v", err)
	}

	query = db.NewQuery()
	if err := query.FetchResource("local:123"); err != nil {
		t.Errorf("error fetching resource: %v", err)
	} else {
		if len(query.Result()) > 0 {
			t.Errorf("after purge shouldn't have found any data. found %v nodes: ", len(query.Result()))
		}
	}
}
