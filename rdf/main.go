// pgdoc wraps a postgresql database into a
// simple document database using the json column
package rdf

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
	"time"
)

type (
	ValueType uint8
	columnDef struct {
		name    string
		kind    string
		notnull string
		pk      bool
		idx     string
	}
	rdfRecord struct {
		resid     uint64
		subject   string
		when      time.Time
		valtype   ValueType
		valint    int64
		valdouble float64
		valtext   string
		valjson   jsonCol
		valref    string
	}
	tableDef struct {
		name string
		def  []columnDef
	}
	Changeset struct {
		tx       *sql.Tx
		owner    *Database
		firstErr error
	}
	RdfNode struct {
		Res     string
		Subject string
		Type    ValueType
		Value   interface{}
	}
	Database struct {
		db        *sql.DB
		reflector reflector.R
	}
	jsonCol struct {
		val interface{}
	}
)

const (
	Invalid = ValueType(0)
	// RDF node holds a string
	String = ValueType(1)
	// RDF node holds a Integer
	Int = ValueType(2)
	// RDF node holds a Double
	Double = ValueType(4)
	// RDF node holds a JSON Document
	Doc = ValueType(8)
	// RDF node holds a ref to another Resource
	Ref = ValueType(16)
)

func (vt ValueType) Valid() bool {
	return vt > Invalid && vt <= Ref
}

func (vt ValueType) String() string {
	switch vt {
	case String:
		return "String"
	case Int:
		return "Int (64 bits)"
	case Double:
		return "Double (64 bits)"
	case Doc:
		return "Document (json)"
	case Ref:
		return "Reference"
	}
	return "Invalid"
}

var (
	errValNotAPointer        = errors.New("value isn't a pointer to a value")
	errAtLeastOneParameter   = errors.New("at least one parameter should be used")
	errCannotStoreValue      = errors.New("cannot store the given value")
	ErrIndexAlreadyExists    = errors.New("index already exists on database")
	errResourceWithoutPrefix = errors.New("resource without a prefix")
	resTableDef              = tableDef{
		name: "!invalid",
		def: []columnDef{
			columnDef{
				name:    "resource",
				kind:    "text",
				notnull: "not null",
				pk:      true,
			},
			columnDef{
				name:    "resid",
				kind:    "bigint",
				notnull: "not null",
				idx:     "default",
			},
		},
	}
	rdfTableDef = tableDef{
		name: "!invalid",
		def: []columnDef{
			columnDef{
				name:    "resid",
				kind:    "bigint",
				notnull: "not null",
				idx:     "default",
			},
			columnDef{
				name:    "subject",
				kind:    "text",
				notnull: "not null",
				idx:     "hash",
			},
			columnDef{
				name:    "valtype",
				kind:    "smallint",
				notnull: "not null",
			},
			columnDef{
				name:    "_when",
				kind:    "timestamp",
				notnull: "not null",
				idx:     "default",
			},
			columnDef{
				name: "valint",
				kind: "bigint",
				idx:  "default",
			},
			columnDef{
				name: "valdouble",
				kind: "double precision",
				idx:  "default",
			},
			columnDef{
				name: "valtext",
				kind: "text",
				idx:  "default",
			},
			columnDef{
				name: "valjson",
				kind: "json",
			},
			columnDef{
				name: "valref",
				kind: "text",
				idx:  "hash",
			},
		},
	}
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

func (d *Database) Begin() (*Changeset, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	return &Changeset{
		tx,
		d,
		nil,
	}, nil
}

func (d *Database) tableNameForResource(resName string) (string, string, error) {
	idx := strings.Index(resName, ":")
	if idx < 0 {
		return "", "", errResourceWithoutPrefix
	}
	prefix := resName[0:idx]
	return d.tableNameForPrefix(prefix)
}

func (d *Database) tableNameForPrefix(prefix string) (string, string, error) {
	return fmt.Sprintf("%v_res", prefix), fmt.Sprintf("%v_rdf", prefix), nil
}

func (d *Database) CreateResourceAlias(name string) error {
	resname, rdfname, err := d.tableNameForPrefix(name)
	if err != nil {
		return err
	}
	rdfDef := tableDef{
		name: rdfname,
		def:  rdfTableDef.def,
	}
	resDef := tableDef{
		name: resname,
		def:  resTableDef.def,
	}
	if err := d.ensure(&resDef); err != nil {
		return err
	}

	if err := d.ensure(&rdfDef); err != nil {
		return err
	}
	return nil
}

// Truncate remove all data from the given table or link and
// all related foreign keys (if any)
func (d *Database) Truncate(prefix string) error {
	resname, rdfname, err := d.tableNameForPrefix(prefix)
	if err != nil {
		return err
	}
	_, err = d.db.Exec(fmt.Sprintf("TRUNCATE %v CASCADE", resname))
	if err == nil {
		_, err = d.db.Exec(fmt.Sprintf("TRUNCATE %v CASCADE", rdfname))
	}
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
