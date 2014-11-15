package rdf

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
)

var (
	colNamesForType = map[ValueType]string{
		Doc:    "valjson",
		String: "valtext",
		Double: "valdouble",
		Int:    "valint",
		Ref:    "valref",
	}
)

func (q *Query) Result() []Node {
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
	if err != nil {
		return err
	}
	ids := q.intersect(results, false)
	if len(ids) == 0 {
		return nil
	}
	err = q.rebuildResources(ids)
	return err
}

func (q *Query) rebuildResources(ids []uint64) error {
	// TODO: we should change this to remove this hard path for tables
	//
	// Since the data is stored across many tables, we should query a view
	// or a union of all tables. For the moment, let's just ignore that fact
	// and keep everything in a single table.
	//
	// Fixing this shouldn't change the API a lot.
	restbl, rdftbl, _ := q.owner.tableNameForPrefix("CHANGEME")
	query := fmt.Sprintf(`select
	res.resource, rdf.resid, rdf.subject, rdf.valtype, rdf._when,
	rdf.valint, rdf.valdouble, rdf.valtext, rdf.valjson, rdf.valref
	from %v res inner join %v rdf on res.resid = rdf.resid
	where rdf.resid in ($1)
	order by rdf.resid, rdf._when`, restbl, rdftbl)

	idsStr := join(func(i int) string {
		return fmt.Sprintf("%d", ids[i])
	}, len(ids), ",")

	rows, err := q.tx.Query(query, idsStr)
	if err != nil {
		return err
	}
	err = q.populateResult(rows)
	defer rows.Close()
	return err
}

func (q *Query) intersect(sets [][]uint64, allowDuplicate bool) []uint64 {
	// TODO: We shouldn't do soo much allocations
	var out []uint64
	if len(sets) == 0 {
		return out
	}
	if len(sets) == 1 {
		out = make([]uint64, len(sets[0]))
		copy(out, sets[0])
		return out
	}
	for _, id := range sets[0] {
		insert := true
		for idx, ids := range sets {
			if idx == 0 {
				// ignore
				continue
			}
			tocheck := idslice(ids)
			if _, ok := tocheck.FindIndex(id); !ok {
				insert = false
			}
		}
		if insert {
			if !allowDuplicate {
				tocheck := idslice(out)
				if _, ok := tocheck.FindIndex(id); ok {
					// we found the id somewhere in the slice
					// and we don't want to duplicate
					continue
				}
			}
			// the id wasn't found or we don't care
			// just insert it
			out = append(out, id)
		}
	}
	return out
}

func (q *Query) fetchIdSet() ([][]uint64, error) {
	// TODO: check the rebuildResources method for more info.
	_, rdftbl, _ := q.owner.tableNameForPrefix("CHANGEME")
	query := fmt.Sprintf(`select rdf.resid
	from %v rdf where `, rdftbl)

	var results [][]uint64
	for _, v := range q.filter {
		err := func() error {
			actualQuery := query + v.formatQuery("rdf")
			rows, err := q.tx.Query(actualQuery, v.Subject, v.Value)
			if err != nil {
				return err
			}
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
	// TODO: check the rebuildResources method for more info
	restbl, rdftbl, _ := q.owner.tableNameForPrefix("CHANGEME")
	query := fmt.Sprintf(`select
	res.resource, rdf.resid, rdf.subject, rdf.valtype, rdf._when,
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
	if in.Err() == nil && len(out) == 0 {
		return out, errNoData
	}
	sort.Sort(idslice(out))
	return out, in.Err()
}

func (q *Query) scanRec(out *rdfRecord, in scanner) error {
	return in.Scan(&out.resource, &out.resid, &out.subject, &out.valtype, &out.when,
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

func (q *Query) recToNode(rec *rdfRecord) Node {
	node := Node{
		Res:     rec.resource,
		Subject: rec.subject,
		When:    rec.when,
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
		tmp := make([]Node, len(q.result), len(q.result)+n)
		copy(tmp, q.result)
		q.result = tmp
	}
}

func (q *Query) Done() error {
	if q.tx == q.owner.db {
		return nil
	}
	if closer, ok := q.tx.(closer); ok {
		return closer.Close()
	}
	return nil
}

func (f *Filter) formatQuery(tbl string) string {
	op := f.Op
	if len(op) == 0 {
		op = Equals
	}
	return fmt.Sprintf(" %v.subject = $1 and %v.%v %v $2", tbl, tbl, f.colname(), op)
}

func (f *Filter) colname() string {
	tp := f.Type
	if tp == Invalid {
		tp, _ = guessTypeForValue(reflect.ValueOf(f.Value))
	}
	if col, has := colNamesForType[f.Type]; has {
		return col
	}
	panic("cannot filter value of type: " + tp.String())
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

// Return the index where id is found for the first time or the index
// where it would be found if it was inside the array.
//
// And also returns true only if id is found at the index returned above.
func (i idslice) FindIndex(id uint64) (int, bool) {
	if len(i) == 0 {
		return 0, false
	}
	idx := sort.Search(i.Len(), func(idx int) bool {
		return i[idx] >= id
	})
	return idx, i[idx] == id
}
