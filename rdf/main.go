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
	"io"
	"reflect"
	"strings"
	"time"
)

type (
	ValueType uint8
	Op        string
	idslice   []uint64
	columnDef struct {
		name    string
		kind    string
		notnull string
		pk      bool
		idx     string
	}
	rdfRecord struct {
		resource  string
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
	querier interface {
		// Query many rows
		Query(string, ...interface{}) (*sql.Rows, error)
		// Query one row
		QueryRow(string, ...interface{}) *sql.Row
	}
	closer interface {
		Close() error
	}
	Changeset struct {
		tx       *sql.Tx
		owner    *Database
		firstErr error
	}
	Node struct {
		Res     string
		Subject string
		Type    ValueType
		When    time.Time
		Value   interface{}
	}
	Query struct {
		owner  *Database
		filter []Filter
		result []Node
		tx     querier
	}
	Filter struct {
		Subject string
		Op      Op
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
	scanner interface {
		Scan(out ...interface{}) error
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

const (
	Equals        = Op("=")
	Greater       = Op(">")
	Less          = Op("<")
	GreaterEquals = Greater + Equals
	LessEquals    = Less + Equals
	NotEqual      = Op("!=")
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
	errValNotAPointer           = errors.New("value isn't a pointer to a value")
	errNotADocument             = errors.New("not a document value")
	errCannotQueryWithoutFilter = errors.New("cannot query without a filter")
	errAtLeastOneParameter      = errors.New("at least one parameter should be used")
	errCannotStoreValue         = errors.New("cannot store the given value")
	ErrIndexAlreadyExists       = errors.New("index already exists on database")
	errResourceWithoutPrefix    = errors.New("resource without a prefix")
	resTableDef                 = tableDef{
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
		db.Close()
		return nil, err
	}
	rdfdb := &Database{db, reflector.R{}}
	// ring0 must exist always, since it could be used
	// to store meta-data about the other rings
	err = rdfdb.createResourceAlias("ring_sys")
	rings, err := rdfdb.getAllRings()
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, r := range rings {
		err := rdfdb.createResourceAlias(r)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	return rdfdb, err
}

func doInsideTransaction(tx *sql.Tx, op func(tx *sql.Tx) error) (err error) {
	defer func() {
		if problem := recover(); problem != nil {
			// a panic, should abort this
			err = tx.Rollback()
			if err != nil {
				err = fmt.Errorf("%v happened when rollingback a transaction. cause [panic]: %v", err, problem)
			} else {
				err = fmt.Errorf("rollback [panic]: %v", problem)
			}
			return
		}

		// no panic, let's check the error
		if err == nil {
			// everything is fine, let's commit
			err = tx.Commit()
		} else {
			// oops, need to rollback
			tmp := tx.Rollback()
			if tmp != nil {
				err = fmt.Errorf("%v happened when rollingback a transaction. cause [error]: %v", tmp, err)
			}
			// if we didn't got an error from rollback, just let the initial
			// error go to the outside
		}
	}()
	err = op(tx)
	return
}

func (d *Database) TruncateDatabase() (err error) {
	rings, err := d.getAllRings()
	if err != nil {
		return err
	}
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	return doInsideTransaction(tx, func(tx *sql.Tx) error {
		for _, r := range rings {
			res, rdf, _ := d.tableNameForRing(r)
			_, err = tx.Exec(fmt.Sprintf("truncate table %v cascade", rdf))
			if err != nil {
				return err
			}
			_, err = tx.Exec(fmt.Sprintf("truncate table %v cascade", res))
			if err != nil {
				return err
			}
		}
		return nil
	})
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
	if prefix == "sys" {
		return d.tableNameForRing("ring_sys")
	}
	// TODO: Implement a proper consistent hashing here
	//
	// The user shouldn't care about WHERE the data is stored,
	// getAllRings returns the list of available rings to store data
	return d.tableNameForRing("ring0")
}

func (d *Database) tableNameForRing(ring string) (string, string, error) {
	return fmt.Sprintf("%v_res", ring), fmt.Sprintf("%v_rdf", ring), nil
}

// Return all rings that can be used to split the data
//
// TODO: at this moment this uses only one ring (ring0)
func (d *Database) getAllRings() ([]string, error) {
	return []string{"ring0"}, nil
}

func (d *Database) resourceNameForUrl(url string) (string, string) {
	idx := strings.Index(url, ":")
	if idx < 0 {
		return "", ""
	}
	prefix := url[0:idx]
	return prefix, url[idx+1:]
}

func (d *Database) NewQuery() Query {
	return Query{
		owner: d,
		tx:    d.db,
	}
}

func (d *Database) createResourceAlias(name string) error {
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

func (jc jsonCol) toPGValue() interface{} {
	if jc.val == nil {
		return nil
	}
	return jc.String()
}

func (jc jsonCol) String() string {
	if jc.val == nil {
		return "{}"
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.Encode(jc.val)
	return string(buf.Bytes())
}

func (jc *jsonCol) Scan(in interface{}) error {
	var buf []byte
	switch in := in.(type) {
	case []byte:
		buf = in
	case string:
		buf = []byte(buf)
	default:
		if in == nil {
			return nil
		}
		return fmt.Errorf("cannot decode value %T into a jsonCol", in)
	}
	if buf == nil || len(buf) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewBuffer(buf))
	if jc.val == nil {
		msg := make(json.RawMessage, len(buf))
		copy(msg, buf)
		jc.val = msg
		return nil
	}
	return dec.Decode(&jc.val)
}

func guessTypeForValue(val reflect.Value) (ValueType, interface{}) {
	vt := removeIndirection(val)
	switch vt.Kind() {
	case reflect.Struct, reflect.Map, reflect.Slice:
		return Doc, vt.Interface()
	case reflect.String:
		return String, vt.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Int, vt.Int()
	case reflect.Float32, reflect.Float64:
		return Double, vt.Float()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return Int, int64(vt.Uint())
	}
	panic(fmt.Sprintf("cannot guess type for %#T", val))
}

func removeIndirection(val reflect.Value) reflect.Value {
	if val.Kind() == reflect.Ptr {
		return removeIndirection(val.Elem())
	}
	return val
}

func join(fn func(i int) string, n int, sep string) string {
	buf := &bytes.Buffer{}
	for i := 0; i < n; i++ {
		if i > 0 {
			io.WriteString(buf, sep)
		}
		io.WriteString(buf, fn(i))
	}
	return string(buf.Bytes())
}
