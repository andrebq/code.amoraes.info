package rdf

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Begin() (*Changeset, error) {
	tx, err := t.owner.db.Begin()
	if err != nil {
		return nil, err
	}
	return &Changeset{
		tx,
		t.name,
		t.rdfname,
	}, nil
}
