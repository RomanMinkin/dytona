package dytona

import (
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/imdario/mergo"
)

const (
	AttributeTypeB    string = "B"
	AttributeTypeBOOL string = "BOOL"
	AttributeTypeBS   string = "BS"
	AttributeTypeL    string = "L"
	AttributeTypeM    string = "M"
	AttributeTypeN    string = "N"
	AttributeTypeNS   string = "NS"
	AttributeTypeNULL string = "NULL"
	AttributeTypeS    string = "S"
	AttributeTypeSS   string = "SS"

	KeyTypeHASH  string = "HASH"
	KeyTypeRANGE string = "RANGE"
)

type Table struct {
	description *dynamodb.TableDescription
	session     *dynamodb.DynamoDB
	newItemFunc func() Itemer
}

func NewTable(name string, newItemFunc func() Itemer) *Table {
	t := &Table{newItemFunc: newItemFunc}

	t.description = &dynamodb.TableDescription{
		TableName:            aws.String(name),
		AttributeDefinitions: t.attributeDefinitions(),
		KeySchema:            t.keySchema(),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	return t
}

func (t *Table) NewItem() Itemer {
	item := t.newItemFunc()

	return item.SetItem(item).
		WithSession(t.session)
}

func (t *Table) WithSession(session *dynamodb.DynamoDB) *Table {
	t.session = session
	return t
}

func (t *Table) Name() string {
	return *t.description.TableName
}

func (t *Table) Description() dynamodb.TableDescription {
	return *t.description
}

func (t *Table) Create() error {
	if out, err := t.session.CreateTable(&dynamodb.CreateTableInput{
		TableName:            t.description.TableName,
		AttributeDefinitions: t.description.AttributeDefinitions,
		KeySchema:            t.description.KeySchema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  t.description.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: t.description.ProvisionedThroughput.WriteCapacityUnits,
		},
	}); err != nil {
		return err

	} else {
		t.description = out.TableDescription
	}

	return nil
}

func (t *Table) Delete() error {
	if out, err := t.session.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: t.description.TableName,
	}); err != nil {
		return err

	} else {
		t.description = out.TableDescription
	}

	return nil
}

func (t *Table) keySchema() []*dynamodb.KeySchemaElement {
	var (
		keys []*dynamodb.KeySchemaElement
		item Itemer       = t.NewItem()
		tp   reflect.Type = reflect.TypeOf(item.GetItem()).Elem()
	)

	for i := 0; i < tp.NumField(); i++ {
		var attributeName, keyType string

		if tp.Field(i).Anonymous && tp.Field(i).Type.Kind() != reflect.Struct {
			continue
		}

		if dynamodbavTagValue, ok := tp.Field(i).Tag.Lookup("dynamodbav"); ok {
			attributeName = strings.Split(dynamodbavTagValue, ",")[0]
		} else {
			continue
		}

		if dynamodbpkTagValue, ok := tp.Field(i).Tag.Lookup("dynamodbpk"); ok {
			dynamodbpkTagValue = strings.ToUpper(dynamodbpkTagValue)

			switch dynamodbpkTagValue {
			case KeyTypeHASH, KeyTypeRANGE:
				keyType = dynamodbpkTagValue
				break
			default:
				continue
				break
			}

		} else {
			continue
		}

		keys = append(keys, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(attributeName),
			KeyType:       aws.String(keyType),
		})
	}

	// setting key for the default `Id` field
	if len(keys) == 0 {
		f, _ := tp.FieldByName("Id")
		attributeName, _ := getFieldAttributeNameAndType(f, nil)
		keys = append(keys, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(attributeName),
			KeyType:       aws.String(KeyTypeHASH),
		})
	}

	return keys
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
		var (
			attributeName, attributeType string
			f                            reflect.StructField = t.Field(i)
		)

		if _, ok := f.Tag.Lookup("dynamodbpk"); !ok {
			continue
		}

		attributeName, attributeType = getFieldAttributeNameAndType(f, adm)

		// using field name here to avoid duplicates,
		// because on field overwrite tag can be different, like `id` -> `_id`
		if attributeName != "" && attributeType != "" {
			adm[t.Field(i).Name] = &dynamodb.AttributeDefinition{
				AttributeName: aws.String(attributeName),
				AttributeType: aws.String(attributeType),
			}
		}
	}

	// setting default `id` field
	if len(adm) == 0 {
		f, _ := t.FieldByName("Id")
		attributeName, attributeType := getFieldAttributeNameAndType(f, adm)

		adm["Id"] = &dynamodb.AttributeDefinition{
			AttributeName: aws.String(attributeName),
			AttributeType: aws.String(attributeType),
		}
	}

	return adm
}

func getFieldAttributeNameAndType(f reflect.StructField, adm map[string]*dynamodb.AttributeDefinition) (attributeName, attributeType string) {
	if f.Anonymous && f.Type.Kind() != reflect.Struct {
		return
	}

	if dynamodbavTagValue, ok := f.Tag.Lookup("dynamodbav"); ok {
		attributeName = strings.Split(dynamodbavTagValue, ",")[0]
	} else {
		return
	}

	if attributeTypeTagValue, ok := f.Tag.Lookup("dynamodbat"); ok {
		switch attributeTypeTagValue {
		case
			AttributeTypeB,
			AttributeTypeBOOL,
			AttributeTypeBS,
			AttributeTypeL,
			AttributeTypeM,
			AttributeTypeN,
			AttributeTypeNS,
			AttributeTypeNULL,
			AttributeTypeS,
			AttributeTypeSS:
			attributeType = attributeTypeTagValue
			break
		}
	}

	if attributeType == "" {
		switch f.Type.Kind() {
		case reflect.Int:
			attributeType = AttributeTypeN
			break
		case reflect.String:
			attributeType = AttributeTypeS
			break
		case reflect.Bool:
			attributeType = AttributeTypeBOOL
			break
		case reflect.Slice:
			attributeType = AttributeTypeL
			break
		case reflect.Map:
			attributeType = AttributeTypeM
			break
		case reflect.Struct:
			switch f.Type {
			case reflect.TypeOf(time.Now()):
				attributeType = AttributeTypeS
				break
			default:
				if err := mergo.MapWithOverwrite(&adm, getAttributeDefinitionMap(f.Type)); err != nil {
					panic(err)
				}
				// Continue here to avoid having "-" fields inherited form nested struct field name
				return
				break
			}
			break
		default:
			attributeType = AttributeTypeS
			break
		}
	}

	return
}
