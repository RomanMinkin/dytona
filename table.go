package dytona

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Table struct {
	config  *dynamodb.CreateTableInput
	session *dynamodb.DynamoDB
	name    string
	newItem func() Itemer
}

func NewTable(name string) *Table {
	return &Table{name: name}
}

func (t *Table) WithSession(session *dynamodb.DynamoDB) *Table {
	t.session = session
	return t
}

func (t *Table) WithNewItem(newItem func() Itemer) *Table {
	t.newItem = newItem
	// item := newItem()

	return t
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) NewItem() Itemer {
	item := t.newItem()

	return item.WithItem(item).
		WithSession(t.session).
		WithTableName(t.name)
}
