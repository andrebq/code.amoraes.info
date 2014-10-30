package rdf

import (
	"database/sql"
	"fmt"
	"sort"
)

func (q *Query) Result() []RdfNode {
	return q.result
}

func (q *Query) CopyFilterFrom(other *Query) *Query {
	q.filter = make([]Filter, len(other.filter))
	copy(q.filter, other.filter)
	return q
}

// Return the list of resources that have all filters as true
func (q *Query) Exec() error {
	results, err := q.fetchIdSet()
	ids := q.intersect(results)
	_ = ids
	return err
}

func (q *Query) intersect(sets [][]uint64) []uint64 {
	return nil
}

func (q *Query) fetchIdSet() ([][]uint64, error) {
	_, rdftbl, _ := q.owner.tableNameForPrefix(q.alias)
	query := fmt.Sprintf(`select rdf.resid
	from %v rdf where `, rdftbl)

	var results [][]uint64
	for _, v := range q.filter {
		err := func() error {
			actualQuery := query + v.formatQuery("rdf")
			rows, err := q.tx.Query(actualQuery, v.Subject, v.Value)
			ids, err := q.scanIdsInto(nil, rows)
			defer rows.Close()
			if err != nil {
				return err
			}
			results = append(results, ids)
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (q *Query) FetchResource(url string) error {
	restbl, rdftbl, _ := q.owner.tableNameForPrefix(q.alias)
	query := fmt.Sprintf(`select
	res.resource, rdf.resid, rdf.subject, rdf.valtype,
	rdf.valint, rdf.valdouble, rdf.valtext, rdf.valjson, rdf.valref
	from %v res inner join %v rdf on res.resid = rdf.resid
	where res.resource = $1
	order by rdf._when`, restbl, rdftbl)

	rows, err := q.tx.Query(query, url)
	if err != nil {
		return err
	}
	err = q.populateResult(rows)
	defer rows.Close()
	return err
}

func (q *Query) scanIdsInto(out []uint64, in *sql.Rows) ([]uint64, error) {
	for in.Next() {
		var id uint64
		err := in.Scan(&id)
		if err != nil {
			return out, err
		}
		out = append(out, id)
	}
	sort.Sort(idslice(out))
	return out, in.Err()
}

func (q *Query) scanRec(out *rdfRecord, in scanner) error {
	return in.Scan(&out.resource, &out.resid, &out.subject, &out.valtype,
		&out.valint, &out.valdouble, &out.valtext, &out.valjson, &out.valref)
}

func (q *Query) populateResult(rows *sql.Rows) error {
	for rows.Next() {
		var rec rdfRecord
		err := q.scanRec(&rec, rows)
		if err != nil {
			return err
		}
		q.addResult(&rec)
	}
	return rows.Err()
}

func (q *Query) AddFilter(f Filter) *Query {
	q.filter = append(q.filter, f)
	return q
}

func (q *Query) addResult(rec *rdfRecord) {
	q.result = append(q.result, q.recToNode(rec))
}

func (q *Query) recToNode(rec *rdfRecord) RdfNode {
	node := RdfNode{
		Res:     rec.resource,
		Subject: rec.subject,
		Type:    ValueType(rec.valtype),
	}
	switch node.Type {
	case String:
		node.Value = rec.valtext
	case Double:
		node.Value = rec.valdouble
	case Doc:
		node.Value = rec.valjson.val
	case Int:
		node.Value = rec.valint
	case Ref:
		node.Value = rec.valref
	default:
		panic(fmt.Sprintf("cannot handle ValueType: %v", node.Type))
	}
	return node
}

func (q *Query) expandResult(n int) {
	if cap(q.result) <= len(q.result)+n {
		// enough space
		tmp := make([]RdfNode, len(q.result), len(q.result)+n)
		copy(tmp, q.result)
		q.result = tmp
	}
}

func (f *Filter) formatQuery(tbl string) string {
	return fmt.Sprintf(" %v.subject = $1 and %v.value %v $2", tbl, tbl, f.Op)
}

func (i idslice) Len() int {
	return len(i)
}

func (i idslice) Less(a, b int) bool {
	return i[a] < i[b]
}

func (i idslice) Swap(a, b int) {
	i[a], i[b] = i[b], i[a]
}
