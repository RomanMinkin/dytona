package dytona

import (
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/imdario/mergo"
)

type Table struct {
	definition  *dynamodb.CreateTableInput
	session     *dynamodb.DynamoDB
	newItemFunc func() Itemer
}

func NewTable(name string, newItemFunc func() Itemer) *Table {
	t := &Table{
		newItemFunc: newItemFunc,
		definition: &dynamodb.CreateTableInput{
			TableName: aws.String(name),
		},
	}

	t.definition.AttributeDefinitions = t.attributeDefinitions()

	return t
}

func (t *Table) NewItem() Itemer {
	item := t.newItemFunc()

	return item.SetItem(item).
		WithSession(t.session).
		WithTableName(*t.definition.TableName)
}

func (t *Table) WithSession(session *dynamodb.DynamoDB) *Table {
	t.session = session
	return t
}

func (t *Table) Name() string {
	return *t.definition.TableName
}

func (t *Table) Definition() dynamodb.CreateTableInput {
	return *t.definition
}

// For table creation process
// Generating dynamodb.AttributeDefinition slice which can be used later for table creation
func (t *Table) attributeDefinitions() []*dynamodb.AttributeDefinition {
	var (
		adm  map[string]*dynamodb.AttributeDefinition
		ads  []*dynamodb.AttributeDefinition
		item Itemer = t.NewItem()
	)

	adm = getAttributeDefinitionMap(reflect.TypeOf(item.GetItem()).Elem())

	for _, attributeDefinition := range adm {
		ads = append(ads, attributeDefinition)
	}
	return ads
}

// Helper function for getting table->item Attributes' Definition from reflected Item type
func getAttributeDefinitionMap(t reflect.Type) map[string]*dynamodb.AttributeDefinition {
	var adm map[string]*dynamodb.AttributeDefinition = make(map[string]*dynamodb.AttributeDefinition)

	for i := 0; i < t.NumField(); i++ {
		var attributeName, attributeType string

		if t.Field(i).Anonymous && t.Field(i).Type.Kind() != reflect.Struct {
			continue
		}

		if dynamodbavTagValue, ok := t.Field(i).Tag.Lookup("dynamodbav"); ok {
			attributeName = strings.Split(dynamodbavTagValue, ",")[0]
		} else {
			continue
		}

		switch t.Field(i).Type.Kind() {
		case reflect.Int:
			attributeType = "N"
			break
		case reflect.String:
			attributeType = "S"
			break
		case reflect.Bool:
			attributeType = "BOOL"
			break
		case reflect.Slice:
			attributeType = "L"
			break
		case reflect.Map:
			attributeType = "M"
			break
		case reflect.Struct:
			switch t.Field(i).Type {
			case reflect.TypeOf(time.Now()):
				attributeType = "S"
				break
			default:
				if err := mergo.MapWithOverwrite(&adm, getAttributeDefinitionMap(t.Field(i).Type)); err != nil {
					panic(err)
				}
				// Continue here to avoid having "-" fields inherited form nested struct field name
				continue
				break
			}
			break
		default:
			attributeType = "S"
			break
		}

		// using field name here to avoid duplicates,
		// because on field overwrite tag can be different, like `id` -> `_id`
		adm[t.Field(i).Name] = &dynamodb.AttributeDefinition{
			AttributeName: aws.String(attributeName),
			AttributeType: aws.String(attributeType),
		}
	}

	return adm
}
