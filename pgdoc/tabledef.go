package pgdoc

import (
	"bytes"
	"database/sql"
	"fmt"
)

func (t *tableDef) create(owner *Database) error {
	db := owner.db
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "create table %v (", t.name)
	for i, col := range t.def {
		if i > 0 {
			fmt.Fprintf(buf, ", ")
		}
		fmt.Fprintf(buf, "%v %v %v", col.name, col.kind, col.notnull)
	}
	fmt.Fprintf(buf, ");\n")
	var haspk bool
	for _, col := range t.def {
		if col.pk {
			haspk = true
			break
		}
	}
	if haspk {
		fmt.Fprintf(buf, "alter table %v add constraint pk_%v primary key (",
			t.name, t.name)
		pkcount := int(0)
		for i, col := range t.def {
			if !col.pk {
				continue
			}
			if i > 0 {
				fmt.Fprintf(buf, ", ")
			}
			fmt.Fprintf(buf, "%v", col.name)
			pkcount++
		}
		fmt.Fprintf(buf, ");\n")
	}

	for _, col := range t.def {
		if len(col.idx) > 0 {
			fmt.Fprintf(buf, "create index idx_%v_%v on %v using %v(%v);\n", t.name, col.name, t.name, col.idx, col.name)
		}
	}
	_, err := db.Exec(string(buf.Bytes()))
	return err
}

func (t *tableDef) exists(owner *Database) (bool, error) {
	db := owner.db
	var out bool
	err := db.QueryRow("select true from pg_tables tbl where tbl.tablename = $1", t.name).Scan(&out)
	if err == sql.ErrNoRows {
		out = false
		err = nil
	}
	return out, err
}
