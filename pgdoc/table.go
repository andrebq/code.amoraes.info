package pgdoc

import (
	"database/sql"
	"fmt"
)

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Load(out interface{}, id string) error {
	if !t.owner.reflector.IsPtr(out) {
		return errValNotAPointer
	}
	return t.query(out, id)
}

// Save will put the given object in the table.
func (t *Table) Save(val interface{}) (string, error) {
	r := t.owner.reflector
	if !r.IsPtr(val) {
		return "", errValNotAPointer
	}
	var id string
	if r.HasField(val, "Id") {
		previd := r.GetField(val, "Id", "").(string)
		if len(previd) == 0 {
			id = t.newId()
			return t.insert(id, val)
		} else {
			if exists, err := t.docExists(id); err != nil {
				return "", err
			} else {
				if exists {
					return t.insert(id, val)
				} else {
					return t.update(id, val)
				}
			}
		}
	}
	return t.insert(id, val)
}

func (t *Table) newId() string {
	return t.owner.newId(t.name)
}

func (t *Table) insert(nid string, val interface{}) (string, error) {
	t.owner.reflector.SetField(val, "Id", nid)
	_, err := t.owner.db.Exec(fmt.Sprintf("insert into %v (docid, body) values ($1, $2)", t.name), nid, jsonCol{val}.String())
	if t.owner.reflector.HasField(val, "Id") {
		t.owner.reflector.SetField(val, "Id", nid)
	}
	return nid, err
}

func (t *Table) update(nid string, val interface{}) (string, error) {
	_, err := t.owner.db.Exec(fmt.Sprintf("update %v set body = $2 where docid = $1", nid, jsonCol{val}.String()))
	return nid, err
}

func (t *Table) query(out interface{}, id string) error {
	return t.owner.db.QueryRow(fmt.Sprintf("select body from %v where docid = $1", t.name), id).Scan(&jsonCol{out})
}

func (t *Table) docExists(id string) (bool, error) {
	var exists bool
	err := t.owner.db.QueryRow(fmt.Sprintf("select true from %v where docid = $1", t.name), id).Scan(&exists)
	if err == sql.ErrNoRows {
		err = nil
		exists = false
	}
	return exists, err
}
