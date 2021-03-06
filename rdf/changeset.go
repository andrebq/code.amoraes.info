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

func (c *Changeset) deleteResource(resName, resTbl, rdfTbl string) error {
	var id uint64
	selId := fmt.Sprintf(`select resid from %v where resource = $1`, resTbl)
	delRdf := fmt.Sprintf(`delete from %v where resid = $1`, rdfTbl)
	delRes := fmt.Sprintf(`delete from %v where resid = $1`, resTbl)

	err := c.tx.QueryRow(selId, resName).Scan(&id)
	if err != nil {
		return err
	}

	_, err = c.tx.Exec(delRdf, id)
	if err != nil {
		return err
	}

	_, err = c.tx.Exec(delRes, id)
	return err
}

func (c *Changeset) newId() (uint64, error) {
	return uint64(time.Now().UnixNano()), nil
}

func (c *Changeset) Purge(url string) error {
	resTbl, rdfTbl, _ := c.owner.tableNameForResource(url)
	err := c.deleteResource(url, resTbl, rdfTbl)
	if err == sql.ErrNoRows {
		err = nil
	}
	return err
}

func (c *Changeset) Save(node *Node) (Node, error) {
	id, _, rdf, err := c.ensureResource(node.Res)
	if err != nil {
		c.pushErr(err)
		return Node{}, err
	}
	rec := c.rdfRecordFrom(id, node)
	err = c.pushErr(c.insertRecord(rdf, &rec))
	ret := *node
	ret.When = rec.when
	return ret, err
}

// Return a Query object that will consider the data updated
// by this changeset.
func (c *Changeset) NewQuery() Query {
	q := c.owner.NewQuery()
	q.tx = c.tx
	return q
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

func (c *Changeset) Abort() error {
	return c.tx.Rollback()
}

func (c *Changeset) Err() error {
	return c.firstErr
}

func (c *Changeset) rdfRecordFrom(id uint64, n *Node) rdfRecord {
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
		var val interface{}
		rec.valtype, val = guessTypeForValue(vt)
		switch rec.valtype {
		case Doc:
			rec.valjson.val = val
		case String:
			rec.valtext = val.(string)
		case Int:
			rec.valint = val.(int64)
		case Double:
			rec.valdouble = val.(float64)
		default:
			panic("cannot handle value of type " + rec.valtype.String())
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
