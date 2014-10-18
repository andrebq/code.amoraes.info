package pgdoc

import (
	"database/sql"
)

type (
	Iterator interface {
		// Move the iterator to the next value.
		//
		// IMPORTANT! The iterator starts behind the first value, ie,
		// before reading the first value you MUST CALL Next.
		//
		// When Next returns false this could mean that no more data
		// is available or an error happend. Call Err() to distinguish
		// between the cases.
		//
		// When Next returns false, Close is called so, iterating using
		// for it.Next() { /* ... */ } will close this iterator, as long
		// the for body don't panic.
		//
		// You can call Close multiple times without any problem.
		Next() bool
		// Scan the next value into out
		Scan(out interface{}) error
		// Return the last error found when calling Scan or ErrClosed,
		// if we don't have more data
		Err() error
		// Close this iterator and return any error, errors that happend
		// while calling Scan aren't returned here.
		//
		// Calls to Close are idempotent
		Close() error
	}

	dbRowsIter struct {
		rows      *sql.Rows
		reflector reflector
	}

	errIter struct {
		err error
	}
)

func newIterator(rows *sql.Rows, r reflector) Iterator {
	return &dbRowsIter{
		rows,
		r,
	}
}

func (d *dbRowsIter) Next() bool {
	return d.rows.Next()
}

func (d *dbRowsIter) Err() error {
	return d.rows.Err()
}

func (d *dbRowsIter) Close() error {
	return d.rows.Close()
}

func (d *dbRowsIter) Scan(out interface{}) error {
	if !d.reflector.isPtr(out) {
		return errValNotAPointer
	}
	jc := jsonCol{out}
	return d.rows.Scan(&jc)
}

func (e errIter) Next() bool                 { return false }
func (e errIter) Err() error                 { return e.err }
func (e errIter) Close() error               { return nil }
func (e errIter) Scan(out interface{}) error { return e.err }
