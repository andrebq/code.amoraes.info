package rdf

import (
	"reflect"
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

func TestCreateUniqueIndex(t *testing.T) {
	db := mustOpenDb(t)
	defer db.Close()

	var tbl *Table
	var err error

	docs := []struct {
		Id   string
		Path string
	}{{
		Path: "a/b.go",
	}, {
		Path: "a/b.go",
	}}

	tbl, err = db.Table("uniquedocs")
	if err != nil {
		t.Fatalf("error creating table: %v", err)
	}
	err = db.Truncate(tbl.Name())
	if err != nil {
		t.Errorf("error truncating table: %v.", err)
	}

	// create the unique index
	err = db.Unique(tbl.Name(), "path", "Path")
	if err != nil && err != ErrIndexAlreadyExists {
		t.Fatalf("error creating unique index: %v", err)
	}

	_, err = tbl.Save(&(docs[0]))
	if err != nil {
		t.Fatalf("error saving the first doc: %v", err)
	}

	_, err = tbl.Save(&(docs[1]))
	if err == nil {
		t.Fatalf("should have caused an error, since the Path is the same")
	}
}

func TestCreateTable(t *testing.T) {
	db := mustOpenDb(t)
	defer db.Close()

	var tbl *Table
	var err error

	if tbl, err = db.Table("mydocs"); err != nil {
		t.Fatalf("error creating table: %v", err)
	}

	if err = db.CreateIndex(tbl.Name(), "name", "Name"); err != nil {
		if err == ErrIndexAlreadyExists {
			err = nil
			// drop and try to recreate
			if err = db.DropIndex(tbl.Name(), "name"); err != nil {
				t.Fatalf("error dropping the index %v: %v", "name", err)
			}

			if err = db.CreateIndex(tbl.Name(), "name", "Name"); err != nil {
				t.Fatalf("error creating index after drop: %v", err)
			}
		} else {
			t.Fatalf("error creating index on database: %v", err)
		}
	}

	person := struct {
		Id   string
		Name string
	}{
		Name: "Bob",
	}

	var id string

	if id, err = tbl.Save(&person); err != nil {
		t.Errorf("error saving person: %v", err)
	} else {
		if len(id) == 0 {
			t.Errorf("should have some id")
		} else if id != person.Id {
			t.Errorf("id's don't match expecting %v got %v", id, person.Id)
		}
	}

	other := struct {
		Id   string
		Name string
	}{}

	if err := tbl.Load(&other, id); err != nil {
		t.Errorf("error loading person: %v", err)
	} else {
		if !reflect.DeepEqual(other, person) {
			t.Errorf("expecting %v got %v", person, other)
		}
	}
}

func TestCreateLink(t *testing.T) {
	db := mustOpenDb(t)
	defer db.Close()

	var tbl *Table
	var err error

	persons := []struct {
		Id   string
		Name string
	}{
		{Name: "Bob"},
		{Name: "Tom"},
	}

	if tbl, err = db.Table("mydocs"); err != nil {
		t.Fatalf("error creating table: %v", err)
	}

	if _, err := tbl.Save(&persons[0]); err != nil {
		t.Fatalf("error saving bob: %v", err)
	}

	if _, err := tbl.Save(&persons[1]); err != nil {
		t.Fatalf("error saving tom: %v", err)
	}

	var lnk *Link
	if lnk, err = db.Link("doclinks"); err != nil {
		t.Fatalf("error creating links: %v", err)
	}

	parent := struct {
		From  string
		To    string
		Label string
		Id    string
		Alive bool
	}{
		From:  persons[0].Id,
		To:    persons[1].Id,
		Label: "father-of",
		Alive: true,
	}

	if id, err := lnk.Connect(&parent); err != nil {
		t.Fatalf("error saving link: %v", err)
	} else {
		if len(id) == 0 {
			t.Fatalf("empty id returned for link")
		} else {
			if id != parent.Id {
				t.Fatalf("didn't updated the id in the parent struct. expecting %v got %v", id, parent.Id)
			}
		}
	}

	zero := struct {
		From  string
		To    string
		Label string
		Id    string
		Alive bool
	}{}
	other := zero
	if err := lnk.Load(&other, parent.Id); err != nil {
		t.Fatalf("error loading link: %v", err)
	} else {
		if !reflect.DeepEqual(other, parent) {
			t.Errorf("objects are different: expecting %v got %v", parent, other)
		}
	}

	it := lnk.From(parent.From)
	count := int(0)
	expectedId := parent.Id
	for it.Next() {
		count++
		other = zero
		if err := it.Scan(&other); err != nil {
			t.Errorf("error scaning from iterator: %v", err)
		} else if !reflect.DeepEqual(other, parent) {
			t.Errorf("data from iteratir is different. expecting %v got %v", parent, other)
		}
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}

	if count == 0 {
		t.Errorf("should have found at least one link")
	}

	it = lnk.To(parent.To)
	count = int(0)
	expectedId = parent.Id
	for it.Next() {
		count++
		other = zero
		if err := it.Scan(&other); err != nil {
			t.Errorf("error scaning from iterator: %v", err)
		} else if !reflect.DeepEqual(other, parent) {
			t.Errorf("data from iteratir is different. expecting %v got %v", parent, other)
		}
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}

	if count == 0 {
		t.Errorf("should have found at least one link")
	}

	it = lnk.Label(parent.Label)
	count = int(0)
	expectedId = parent.Id
	for it.Next() {
		if err := it.Scan(&other); err != nil {
			t.Errorf("error scaning from iterator: %v", err)
		} else {
			if other.Id == expectedId {
				count++
			}
		}

	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}

	if count != 1 {
		t.Fatalf("should have found only one link with ID: %v , but found %v", expectedId, count)
	}

	_ = tbl
	_ = lnk
}
