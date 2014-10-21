// pgdoc wraps a postgresql database into a
// simple document database using the json column
package pgdoc

import (
	"amoraes.info/pgdoc/reflector"
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
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
		reflector reflector.R
	}
	jsonCol struct {
		val interface{}
	}
)

var (
	errValNotAPointer      = errors.New("value isn't a pointer to a value")
	errAtLeastOneParameter = errors.New("at least one parameter should be used")
	ErrIndexAlreadyExists  = errors.New("index already exists on database")
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
	return &Database{db, reflector.R{}}, nil
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

// Truncate remove all data from the given table or link and
// all related foreign keys (if any)
func (d *Database) Truncate(tblOrLink string) error {
	_, err := d.db.Exec(fmt.Sprintf("TRUNCATE %v CASCADE", tblOrLink))
	return err
}

func (d *Database) Unique(tableOrLink string, idxName string, propPath ...string) error {
	if exists, err := d.indexExistsOn(tableOrLink, idxName); err != nil {
		return err
	} else {
		if exists {
			return ErrIndexAlreadyExists
		}
	}
	return d.createIndex(tableOrLink, idxName, true, propPath...)
}

func (d *Database) CreateIndex(tableOrLink string, idxName string, propPath ...string) error {
	if exists, err := d.indexExistsOn(tableOrLink, idxName); err != nil {
		return err
	} else {
		if exists {
			return ErrIndexAlreadyExists
		}
	}
	return d.createIndex(tableOrLink, idxName, false, propPath...)
}

func (d *Database) DropIndex(tableOrLink string, idxName string) error {
	return d.dropIndex(tableOrLink, idxName)
}

func (d *Database) indexExistsOn(tblLnk string, idxname string) (bool, error) {
	var out bool
	err := d.db.QueryRow("select true from pg_indexes where tablename = $1 and indexname = $2",
		tblLnk, fmt.Sprintf("idx_%v_%v", tblLnk, idxname)).Scan(&out)
	if err == sql.ErrNoRows {
		err = nil
	}
	return out, err
}

func (d *Database) dropIndex(tblName, idxName string) error {
	_, err := d.db.Exec(fmt.Sprintf("DROP INDEX idx_%v_%v", tblName, idxName))
	return err
}

func (d *Database) createIndex(tblLnk string, idxname string, unique bool, propPath ...string) error {
	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE"
	}
	cmd := fmt.Sprintf("CREATE %v INDEX idx_%v_%v on %v ((body#>>'{%v}'));", uniqueStr, tblLnk, idxname, tblLnk, strings.Join(propPath, ","))
	_, err := d.db.Exec(cmd)
	return err
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
