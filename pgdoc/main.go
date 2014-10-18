// pgdoc wraps a postgresql database into a
// simple document database using the json column
package pgdoc

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
)

type (
	columnDef struct {
		name    string
		kind    string
		notnull string
		pk      bool
		idx     string
	}
	tableDef struct {
		name string
		def  []columnDef
	}
	Table struct {
		name  string
		owner *Database
	}

	Link struct {
		name  string
		owner *Database
	}

	Database struct {
		db        *sql.DB
		reflector reflector
	}

	jsonCol struct {
		val interface{}
	}
)

var (
	errValNotAPointer      = errors.New("value isn't a pointer to a value")
	errAtLeastOneParameter = errors.New("at least one parameter should be used")
)

func OpenDatabase(user, password, database, host string) (*Database, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("dbname=%v password=%v user=%v host=%v sslmode=disable", database, user, password, host))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return &Database{db, reflector{}}, nil
}

func (d *Database) Table(name string) (*Table, error) {
	td := tableDef{
		name: name,
		def: []columnDef{
			columnDef{
				name:    "docid",
				kind:    "varchar(40)",
				pk:      true,
				notnull: "not null",
			},
			columnDef{
				name:    "body",
				kind:    "json",
				notnull: "not null",
			},
		},
	}
	if err := d.ensure(&td); err != nil {
		return nil, err
	}
	return &Table{name, d}, nil
}

func (d *Database) Link(name string) (*Link, error) {
	td := tableDef{
		name: name,
		def: []columnDef{
			columnDef{
				name:    "linkid",
				kind:    "varchar(40)",
				notnull: "not null",
				pk:      true,
			},
			columnDef{
				name:    "_from",
				kind:    "varchar(40)",
				notnull: "not null",
				idx:     "hash",
			},
			columnDef{
				name:    "_to",
				kind:    "varchar(40)",
				notnull: "not null",
				idx:     "hash",
			},
			columnDef{
				name:    "label",
				kind:    "varchar(100)",
				notnull: "not null",
				idx:     "hash",
			},
			columnDef{
				name:    "body",
				kind:    "json",
				notnull: "not null",
			},
		},
	}
	if err := d.ensure(&td); err != nil {
		return nil, err
	}
	return &Link{name, d}, nil
}

func (d *Database) ensure(def *tableDef) error {
	var exists bool
	var err error
	if exists, err = def.exists(d); err != nil {
		return err
	}
	if exists {
		return nil
	}
	return def.create(d)
}

func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) newId(prefix string) string {
	return uuid.New()
}

func (jc jsonCol) String() string {
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.Encode(jc.val)
	return string(buf.Bytes())
}

func (jc jsonCol) Scan(in interface{}) error {
	var buf []byte
	switch in := in.(type) {
	case []byte:
		buf = in
	case string:
		buf = []byte(buf)
	default:
		return fmt.Errorf("cannot decode value %T into a jsonCol", in)
	}
	dec := json.NewDecoder(bytes.NewBuffer(buf))
	return dec.Decode(jc.val)
}
