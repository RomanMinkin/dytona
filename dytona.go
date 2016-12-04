package dytona

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var ErrorAlreadyDialed error = errors.New("DynamoDB connection already dialed")

func NewDytona(id, secret, endpoint, region string) *Dytona {
	return &Dytona{
		config: aws.NewConfig().
			WithCredentials(credentials.NewStaticCredentials(id, secret, "")).
			WithEndpoint(endpoint).
			WithRegion(region),
		registry: make(map[string]*Table),
	}
}

func NewConfig() *aws.Config {
	return aws.NewConfig()
}

type Dytona struct {
	config   *aws.Config
	session  *dynamodb.DynamoDB
	registry map[string]*Table
}

func (d *Dytona) Dial(cfgs ...*aws.Config) error {
	if d.session != nil {
		return ErrorAlreadyDialed
	}

	d.session = dynamodb.New(session.New(d.config), cfgs...)

	return nil
}

func (d *Dytona) GetSession() *dynamodb.DynamoDB {
	return d.session
}

func (d *Dytona) RegisterTable(tableName string, newItemFunc func() Itemer) *Table {
	item := newItemFunc()
	if item == nil {
		panic("dytona.SetTable: item can not be nil")
	}

	tableName = strings.ToLower(tableName)

	t := NewTable(strings.ToLower(tableName), newItemFunc).
		WithSession(d.session)

	d.registry[tableName] = t
	return t
}

func (d *Dytona) Table(tableName string) *Table {
	return d.registry[strings.ToLower(tableName)]
}

// taken form https://play.golang.org/p/Qi_BUiz2sr
// func iterate(v reflect.Value) {
// 	typ := v.Type()
// 	if typ.Kind() == reflect.Ptr {
// 		typ = typ.Elem()
// 		v = v.Elem()
// 	}
// 	// Only structs are supported
// 	if typ.Kind() != reflect.Struct {
// 		return
// 	}

// 	// loop through the struct's fields and set the map
// 	for i := 0; i < typ.NumField(); i++ {
// 		p := typ.Field(i)
// 		if !p.Anonymous {
// 			if p.Name != "_" {
// 				fmt.Println(">>>", p.Name, v.Field(i), v.Field(i).IsNil())
// 			}
// 		} else { // Anonymus structues
// 			iterate(v.Field(i).Addr())
// 		}
// 	}

// }
