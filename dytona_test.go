package dytona

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestNewDytonaConnectionOk(t *testing.T) {
	d := NewDytona("1", "2", "http://localhost:8000", "us-east-1")

	err := d.Dial(NewConfig().WithMaxRetries(0))

	assert.Nil(t, err)
	assert.NotNil(t, d.session)

	if _, err = d.session.ListTables(&dynamodb.ListTablesInput{}); err != nil {
		assert.Nil(t, err, err.Error())
	}
}

func TestNewDytonaConnectionFail(t *testing.T) {
	d := NewDytona("1", "2", "http://badhost", "us-east-1")

	assert.Nil(t, d.session)
	assert.Nil(t, d.Dial(NewConfig().WithMaxRetries(0)))

	_, err := d.session.ListTables(&dynamodb.ListTablesInput{})
	assert.NotNil(t, err, err)
	assert.Equal(t, "RequestError", err.(awserr.Error).Code())
	assert.Equal(t, "send request failed", err.(awserr.Error).Message())
	assert.NotNil(t, d.session)
}

func TestNewDytonaDoubleDial(t *testing.T) {
	d := NewDytona("1", "2", "http://localhost:8000", "us-east-1")

	err := d.Dial(NewConfig().WithMaxRetries(0))
	assert.Nil(t, err)
	assert.NotNil(t, d.session)

	err = d.Dial()
	assert.Equal(t, ErrorAlreadyDialed, err)
	assert.NotNil(t, d.session)
}

func TestRegister(t *testing.T) {
	d := NewDytona("1", "2", "http://localhost:8000", "us-east-1")

	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Name string `json:"name" dynamodbav:"name"`
	}

	table := d.RegisterTable("users", func() Itemer {
		return &User{}
	})
	assert.IsType(t, &Table{}, table)
	assert.IsType(t, &Table{}, d.Table("users"))

	u := d.Table("users").NewItem()
	assert.IsType(t, &User{}, u)
}
