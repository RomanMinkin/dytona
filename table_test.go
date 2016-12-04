package dytona

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Name string `json:"name" dynamodbav:"name"`
	}

	tbl := &Table{
		name:    "users",
		newItem: func() Itemer { return &User{} },
	}

	assert.Equal(t, "users", tbl.Name())
	assert.IsType(t, &User{}, tbl.NewItem())
}
