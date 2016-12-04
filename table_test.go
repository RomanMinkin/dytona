package dytona

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("c_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("u_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("deleted"),
		AttributeType: aws.String("BOOL"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("string"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("int"),
		AttributeType: aws.String("N"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("time"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_int"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_float"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_bool"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_string"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_time"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_int"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_float"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_bool"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_time"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_int_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_bool_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_float_string"),
		AttributeType: aws.String("M"),
	})

	assert.Len(t, ad, 21, "AttributeDefinitions lenght does should match")
}

func TestAttributeDefinitionsOverwrite(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Id   int `json:"_id" dynamodbav:"_id"`
	}

	tbl := NewTable("users", func() Itemer {
		return &User{}
	})

	ad := tbl.attributeDefinitions()
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("_id"),
		AttributeType: aws.String("N"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("c_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("u_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("deleted"),
		AttributeType: aws.String("BOOL"),
	})
	assert.Len(t, ad, 4, "AttributeDefinitions lenght does should match")

	assert.NotContains(t, ad, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String("N"),
	})
}
