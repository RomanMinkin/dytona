package dytona

import (
	"errors"
	"testing"

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
	u.SetItem(u)

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
	u.SetItem(u)

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
	u.SetItem(u)

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
	u.SetItem(u)

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
