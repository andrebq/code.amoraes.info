package rdf

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

func (c *Changeset) insertRecord(rdfTbl string, rec *rdfRecord) error {
	if !rec.valtype.Valid() {
		return errCannotStoreValue
	}
	query := fmt.Sprintf(`insert into %v(resid, subject, valtype, _when, valint, valdouble, valtext, valjson, valref) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, rdfTbl)
	_, err := c.tx.Exec(query, rec.resid, rec.subject, rec.valtype, rec.when, rec.valint, rec.valdouble, rec.valtext, rec.valjson.toPGValue(), rec.valref)
	return err
}

func (c *Changeset) newId() (uint64, error) {
	return uint64(time.Now().UnixNano()), nil
}

func (c *Changeset) Save(node *RdfNode) error {
	id, _, rdf, err := c.ensureResource(node.Res)
	if err != nil {
		c.pushErr(err)
		return err
	}
	rec := c.rdfRecordFrom(id, node)
	return c.pushErr(c.insertRecord(rdf, &rec))
}

func (c *Changeset) pushErr(err error) error {
	if c.firstErr == nil {
		c.firstErr = err
	}
	return err
}

func (c *Changeset) Done() error {
	if c.firstErr != nil {
		return c.tx.Rollback()
	}
	return c.pushErr(c.tx.Commit())
}

func (c *Changeset) Err() error {
	return c.firstErr
}

func (c *Changeset) rdfRecordFrom(id uint64, n *RdfNode) rdfRecord {
	rec := rdfRecord{
		resid:   id,
		subject: n.Subject,
		when:    time.Now(),
	}
	vt := reflect.ValueOf(n.Value)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}
	if n.Type.Valid() && n.Type == Ref {
		rec.valref = n.Value.(string)
		rec.valtype = n.Type
	} else {
		switch vt.Kind() {
		case reflect.Struct, reflect.Map, reflect.Slice:
			rec.valtype = Doc
			rec.valjson.val = n.Value
		case reflect.String:
			rec.valtype = String
			rec.valtext = n.Value.(string)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			rec.valtype = Int
			rec.valint = vt.Int()
		case reflect.Float32, reflect.Float64:
			rec.valtype = Double
			rec.valdouble = vt.Float()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			rec.valtype = Int
			rec.valint = int64(vt.Uint())
		}
	}
	return rec
}

func (c *Changeset) ensureResource(name string) (uint64, string, string, error) {
	res, rdf, err := c.owner.tableNameForResource(name)
	query := fmt.Sprintf("select resid from %v where resource = $1", res)
	var id uint64
	err = c.tx.QueryRow(query, name).Scan(&id)
	if err == sql.ErrNoRows {
		// insert
		query := fmt.Sprintf("insert into %v (resource, resid) values ($1, $2)", res)
		id, err = c.newId()
		if err != nil {
			return 0, res, rdf, err
		}
		_, err := c.tx.Exec(query, name, id)
		if err != nil {
			id = 0
		}
		return id, res, rdf, err
	} else if err != nil {
		return 0, res, rdf, err
	}
	return id, res, rdf, err
}
