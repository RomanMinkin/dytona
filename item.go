package dytona

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/imdario/mergo"
)

type Itemer interface {
	// GetId() bson.ObjectId
	// SetId(bson.ObjectId)

	// SetCreatedAt(time.Time)
	// GetCreatedAt() time.Time

	// SetUpdatedAt(time.Time)
	// GetUpdatedAt() time.Time

	// SetDeleted(bool)
	// IsDeleted() bool

	// SetCollection(*mgo.Collection)
	// SetItem(item Itemer)
	// SetConnection(*Connection)
	WithTableName(tableName string) Itemer
	WithSession(session *dynamodb.DynamoDB) Itemer

	GetItem() Itemer
	SetItem(item Itemer) Itemer

	Get(field string) (interface{}, error)
	Set(field string, value interface{}) bool

	Save() error

	get(field string) (rValue reflect.Value, tag string, found bool)
	// Update(interface{}) (error, map[string]interface{})
	// Validate(...interface{}) (bool, []error)
	// DefaultValidate() (bool, []error)
}

type Item struct {
	item      Itemer             `json:"-" bson:"-"`
	tableName string             `json:"-" bson:"-"`
	session   *dynamodb.DynamoDB `json:"-" bson:"-"`

	Id        string    `json:"id" dynamodbav:"id"`
	CreatedAt time.Time `json:"created_at" dynamodbav:"c_at"`
	UpdatedAt time.Time `json:"updated_at" dynamodbav:"u_at"`
	Deleted   bool      `json:"deleted" dynamodbav:"deleted,omitempty"`
}

// Applying Itemer interface
var _ Itemer = (*Item)(nil)

// Getting a filed reflect value by string name
func (i *Item) get(field string) (rValue reflect.Value, tag string, found bool) {
	rValue = reflect.ValueOf(i.item).Elem().FieldByName(field)

	reflectedField, found := reflect.TypeOf(i.item).Elem().FieldByName(field)
	tag = strings.Split(reflectedField.Tag.Get("dynamodbav"), ",")[0]

	return rValue, tag, found
}

func (i *Item) GetItem() Itemer {
	return i.item
}

func (i *Item) SetItem(item Itemer) Itemer {
	i.item = item
	return i.item
}

func (i *Item) WithTableName(tableName string) Itemer {
	i.tableName = tableName
	return i.item
}

func (i *Item) WithSession(session *dynamodb.DynamoDB) Itemer {
	i.session = session
	return i.item
}

func (i *Item) Marshal() (map[string]*dynamodb.AttributeValue, error) {
	e := dynamodbattribute.NewEncoder(func(e *dynamodbattribute.Encoder) {
		e.NullEmptyString = false
	})

	av, err := e.Encode(i)
	if err != nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	avi, err := e.Encode(i.item)
	if err != nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	err = mergo.MapWithOverwrite(&av.M, avi.M)
	if err != nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	if av == nil || av.M == nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	return av.M, nil
}

func (i *Item) Get(field string) (interface{}, error) {
	if rValue, _, found := i.get(field); !found {
		return nil, errors.New(fmt.Sprintf("Field with name '%s' not found", field))

	} else {
		return rValue.Interface(), nil
	}
}

func (i *Item) Set(field string, value interface{}) bool {
	// Finding reflected value
	rValue, _, found := i.get(field)
	if !found {
		return false
	}

	// Type concictency check
	// if rValue.Type() != reflect.TypeOf(value) {
	// 	return false
	// }

	// Assigning a new value
	rValue.Set(reflect.ValueOf(value))

	// Checking if value was actually set
	if rValue, _, _ := i.get(field); rValue.Interface() == value {
		return true
	}

	return false
}

func (i *Item) Save() error {
	// reflectStruct := reflect.ValueOf(d.item).Elem()

	fmt.Printf("reflectStruct: %#v\n", i)

	return nil
}
