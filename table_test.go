package dytona

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Name string `json:"name" dynamodbav:"name"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	assert.Equal(t, "users", tbl.Name())
	assert.IsType(t, &User{}, tbl.NewItem())
}

func TestAttributeDefinitionsWithEmptyValues(t *testing.T) {
	type User struct {
		Item            `json:"-" dynamodbav:"-"`
		String          string               `json:"string" dynamodbav:"string"`
		Int             int                  `json:"int" dynamodbav:"int"`
		Float           float64              `json:"float" dynamodbav:"float"`
		Time            time.Time            `json:"time" dynamodbav:"time"`
		SliceInt        []int                `json:"slice_int" dynamodbav:"slice_int"`
		SliceFloat      []float64            `json:"slice_float" dynamodbav:"slice_float"`
		SliceBool       []bool               `json:"slice_bool" dynamodbav:"slice_bool"`
		SliceString     []string             `json:"slice_string" dynamodbav:"slice_string"`
		SliceTime       []time.Time          `json:"slice_time" dynamodbav:"slice_time"`
		MapStringInt    map[string]int       `json:"map_string_int" dynamodbav:"map_string_int"`
		MapStringFloat  map[string]float64   `json:"map_string_float" dynamodbav:"map_string_float"`
		MapStringBool   map[string]bool      `json:"map_string_bool" dynamodbav:"map_string_bool"`
		MapStringString map[string]string    `json:"map_string_string" dynamodbav:"map_string_string"`
		MapStringTime   map[string]time.Time `json:"map_string_time" dynamodbav:"map_string_time"`
		MapIntString    map[int]string       `json:"map_int_string" dynamodbav:"map_int_string"`
		MapBoolString   map[bool]string      `json:"map_bool_string" dynamodbav:"map_bool_string"`
		MapFloatString  map[float64]string   `json:"map_float_string" dynamodbav:"map_float_string"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ad := tbl.attributeDefinitions()
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String("S"),
	})

	assert.Len(t, ad, 1)
}

func TestAttributeDefinitionsOverwrite(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Id   int `json:"_id" dynamodbav:"_id"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ad := tbl.Description().AttributeDefinitions
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("_id"),
		AttributeType: aws.String("N"),
	})
	assert.Len(t, ad, 1)

	assert.NotContains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String("N"),
	})
}

func TestKeySchemaDefault(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ks := tbl.Description().KeySchema
	assert.Len(t, ks, 1)
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("id"),
		KeyType:       aws.String("HASH"),
	})
}

func TestKeySchemaDefaultOverwrite(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Id   int `json:"_id" dynamodbav:"_id"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ks := tbl.Description().KeySchema
	assert.Len(t, ks, 1)
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("_id"),
		KeyType:       aws.String("HASH"),
	})

	ad := tbl.Description().AttributeDefinitions
	assert.Len(t, ad, 1)
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("_id"),
		AttributeType: aws.String("N"),
	})
}

func TestKeySchemaCustomHASH(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		UUID string `json:"uuid" dynamodbav:"uuid" dynamodbpk:"HASH"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ks := tbl.Description().KeySchema
	assert.Len(t, ks, 1)
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("uuid"),
		KeyType:       aws.String("HASH"),
	})

	ad := tbl.Description().AttributeDefinitions
	assert.Len(t, ad, 1)
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("uuid"),
		AttributeType: aws.String("S"),
	})
}

func TestKeySchemaCustomHASHandRANGE(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		UUID string    `json:"uuid" dynamodbav:"uuid" dynamodbpk:"HASH"`
		Date time.Time `json:"time" dynamodbav:"time" dynamodbpk:"RANGE"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ks := tbl.Description().KeySchema
	assert.Len(t, ks, 2)
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("uuid"),
		KeyType:       aws.String("HASH"),
	})
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("time"),
		KeyType:       aws.String("RANGE"),
	})

	ad := tbl.Description().AttributeDefinitions
	assert.Len(t, ad, 2)
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("uuid"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("time"),
		AttributeType: aws.String("S"),
	})
}

func TestKeySchemaCustomHASHandRANGELowercase(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		UUID string    `json:"uuid" dynamodbav:"uuid" dynamodbpk:"hash"`
		Date time.Time `json:"time" dynamodbav:"time" dynamodbpk:"range"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ks := tbl.Description().KeySchema
	assert.Len(t, ks, 2)
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("uuid"),
		KeyType:       aws.String("HASH"),
	})
	assert.Contains(t, ks, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("time"),
		KeyType:       aws.String("RANGE"),
	})

	ad := tbl.Description().AttributeDefinitions
	assert.Len(t, ad, 2)
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("uuid"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("time"),
		AttributeType: aws.String("S"),
	})
}

func TestCreate(t *testing.T) {
	type User struct {
		Item   `json:"-" dynamodbav:"-"`
		String string `json:"string" dynamodbav:"string"`
	}

	d := NewDytona("key", "secret", "http://localhost:8000", "us-east-1")
	d.Dial(NewConfig().WithMaxRetries(0))

	tbl := NewTable("users", func() Itemer {
		return &User{String: "Bob"}
	}).WithSession(d.session)

	if err := tbl.Create(); err != nil {
		assert.Nil(t, err, err.(awserr.Error).Error())
	}

	if err := tbl.Delete(); err != nil {
		assert.Nil(t, err, err.(awserr.Error).Error())
	}
}
