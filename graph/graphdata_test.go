package graph

type (
	User struct {
		Id    string
		Email string
	}

	Permission struct {
		Id   string
		Desc string
		Cmd  string
	}

	Access struct {
		Id         string
		User       string `pgdoc:"From"`
		Permission string `pgdoc:"To"`
	}
)
