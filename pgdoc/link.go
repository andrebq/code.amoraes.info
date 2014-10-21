package pgdoc

import (
	"bytes"
	"errors"
	"fmt"
)

func (l *Link) Name() string {
	return l.name
}

func (l *Link) Load(out interface{}, id string) error {
	if !l.owner.reflector.IsPtr(out) {
		return errValNotAPointer
	}
	return l.queryById(out, id)
}

func (l *Link) From(from string) Iterator {
	return l.LoadMany(from, "", "")
}

func (l *Link) To(to string) Iterator {
	return l.LoadMany("", to, "")
}

func (l *Link) Label(label string) Iterator {
	return l.LoadMany("", "", label)
}

func (l *Link) LoadMany(from, to, label string) Iterator {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "select body from %v where", l.name)
	params := []struct {
		name string
		val  string
	}{
		{"_from", from},
		{"_to", to},
		{"label", label},
	}
	parray := make([]interface{}, 0, 3)
	for _, v := range params {
		if len(v.val) > 0 {
			if len(parray) > 0 {
				fmt.Fprintf(buf, " AND ")
			}

			fmt.Fprintf(buf, " (%v = $%v) ", v.name, len(parray)+1)
			parray = append(parray, v.val)
		}
	}

	if len(parray) == 0 {
		return errIter{errAtLeastOneParameter}
	}

	rows, err := l.owner.db.Query(string(buf.Bytes()), parray...)
	if err != nil {
		return errIter{err}
	}
	return newIterator(rows, l.owner.reflector)
}

// Save will put the given object in the table.
func (l *Link) Connect(val interface{}) (string, error) {
	r := l.owner.reflector
	if !r.IsPtr(val) {
		return "", errValNotAPointer
	}

	id := r.GetFieldOrTag(val, "Id", `pgdoc:"Id"`, "").(string)
	from := r.GetFieldOrTag(val, "From", `pgdoc:"From"`, "").(string)
	to := r.GetFieldOrTag(val, "To", `pgdoc:"To"`, "").(string)
	label := r.GetFieldOrTag(val, "Label", `pgdoc:"Label"`, "").(string)

	if len(label) == 0 {
		_, label = r.GetTypeName(val)
	}

	if len(from) == 0 || len(to) == 0 || len(label) == 0 {
		return "", errors.New("all links MUST HAVE a valid From, To and Label fields")
	}

	if len(id) > 0 {
		return l.update(id, from, to, label, val)
	} else {
		return l.insert(id, from, to, label, val)
	}
}

func (l *Link) newId() string {
	return l.owner.newId(l.name)
}

func (l *Link) update(id, from, to, label string, val interface{}) (string, error) {
	id = l.newId()
	l.owner.reflector.SetField(val, "Id", id)
	_, err := l.owner.db.Exec(fmt.Sprintf("update %v set _from = $2, _to = $3, label = $4, body = $5 where linkid = $1", l.name), id, from, to, label, jsonCol{val}.String())
	return id, err
}

func (l *Link) insert(id, from, to, label string, val interface{}) (string, error) {
	id = l.newId()
	l.owner.reflector.SetField(val, "Id", id)
	_, err := l.owner.db.Exec(fmt.Sprintf("insert into %v (linkid, _from, _to, label, body) values ($1, $2, $3, $4, $5)", l.name), id, from, to, label, jsonCol{val}.String())
	return id, err
}

func (l *Link) queryById(out interface{}, id string) error {
	col := jsonCol{out}
	return l.owner.db.QueryRow(fmt.Sprintf("select body from %v where linkid = $1", l.name), id).Scan(&col)
}
