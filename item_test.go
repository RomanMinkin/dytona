package dytona

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInterface(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Name string `json:"name" dynamodbav:"name"`
	}

	assert.Implements(t, (*Itemer)(nil), new(User))
}

func TestGetSet(t *testing.T) {
	type User struct {
		Item     `json:"-" dynamodbav:"-"`
		Name     string `json:"name" dynamodbav:"name"`
		JsonOnly bool   `json:"json_only"`
	}
	u := &User{Name: "Roman", JsonOnly: true}
	u.WithItem(u)

	{
		v, err := u.Get("Name")
		assert.Nil(t, err)
		assert.Equal(t, "Roman", v.(string))

	}

	{
		v, err := u.Get("NotAField")
		assert.Equal(t, errors.New("Field with name 'NotAField' not found"), err)
		assert.Nil(t, v)
	}

	{
		assert.True(t, u.Set("Name", "Boris"))

		v, err := u.Get("Name")
		assert.Nil(t, err)
		assert.Equal(t, "Boris", v.(string))

	}

	{
		v, err := u.Get("JsonOnly")
		assert.Nil(t, err)
		assert.True(t, v.(bool))
	}
}

func TestPrivateGet(t *testing.T) {
	type User struct {
		Item     `json:"-" dynamodbav:"-"`
		Name     string `json:"name" dynamodbav:"name"`
		JsonOnly bool   `json:"json_only"`
	}
	u := &User{Name: "Roman", JsonOnly: true}
	u.WithItem(u)

	{
		rValue, tag, found := u.get("Name")
		assert.Equal(t, "Roman", rValue.Interface().(string), "Reflected field value should match")
		assert.Equal(t, "name", tag, "Field's tag name should match")
		assert.True(t, found, "Field should be found")
	}

	{
		rValue, tag, found := u.get("JsonOnly")
		assert.True(t, rValue.Interface().(bool), "Reflected field value should match")
		assert.Equal(t, "", tag, "Tag should ne be returned only for fields  with a 'dynamodbav' tag")
		assert.True(t, found, "Field should be found")
	}
}

func TestMarshal(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
	}
	u := &User{}
	u.WithItem(u)

	// Should Overwrite id field
	m, err := u.Marshal()
	assert.Nil(t, err)
	assert.NotNil(t, m)
	assert.Nil(t, m["id"].NULL)
	assert.NotNil(t, m["id"].S)
	assert.Equal(t, "", *m["id"].S, "Should not be a nil value")
	assert.NotNil(t, m["c_at"].S)
	assert.Equal(t, "0001-01-01T00:00:00Z", *m["c_at"].S)
	assert.NotNil(t, m["u_at"].S)
	assert.Equal(t, "0001-01-01T00:00:00Z", *m["u_at"].S)

}

func TestMarshalOverwriteWithIdField(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Id   int    `json:"id" dynamodbav:"id"`
		Name string `json:"name" dynamodbav:"name"`
	}
	u := &User{Id: 1, Name: "Roman"}
	u.WithItem(u)

	// Should Overwrite id field
	m, err := u.Marshal()
	assert.Nil(t, err)
	// dynamic values
	assert.Nil(t, m["id"].NULL)
	assert.NotNil(t, m["id"].N)
	assert.Equal(t, "1", *m["id"].N)
	assert.NotNil(t, m["name"].S)
	assert.Equal(t, "Roman", *m["name"].S)
	// default values
	assert.NotNil(t, m["c_at"].S)
	assert.Equal(t, "0001-01-01T00:00:00Z", *m["c_at"].S)
	assert.NotNil(t, m["u_at"].S)
	assert.Equal(t, "0001-01-01T00:00:00Z", *m["u_at"].S)

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

	// Empty object
	u := &User{
	// Name: "Roman",
	// // Points: []int{1, 2, 3},
	}
	u.WithItem(u)

	asd := u.AttributeDefinitions()
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("c_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("u_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("deleted"),
		AttributeType: aws.String("BOOL"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("string"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("int"),
		AttributeType: aws.String("N"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("time"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_int"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_float"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_bool"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_string"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("slice_time"),
		AttributeType: aws.String("L"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_int"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_float"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_bool"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_string_time"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_int_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_bool_string"),
		AttributeType: aws.String("M"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("map_float_string"),
		AttributeType: aws.String("M"),
	})

	assert.Len(t, asd, 21, "AttributeDefinitions lenght does should match")
}

func TestAttributeDefinitionsOverwrite(t *testing.T) {
	type User struct {
		Item `json:"-" dynamodbav:"-"`
		Id   int `json:"_id" dynamodbav:"_id"`
	}

	u := &User{}
	u.WithItem(u)
	asd := u.AttributeDefinitions()
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("_id"),
		AttributeType: aws.String("N"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("c_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("u_at"),
		AttributeType: aws.String("S"),
	})
	assert.Contains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("deleted"),
		AttributeType: aws.String("BOOL"),
	})
	assert.NotContains(t, asd, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String("N"),
	})
	assert.Len(t, asd, 4, "AttributeDefinitions lenght does should match")
}
