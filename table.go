package dytona

import (
	"reflect"
	"strconv"
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

	KeyTypeHASH               string = "HASH"
	KeyTypeRANGE              string = "RANGE"
	KeyProjectionTypeKEYSONLY string = "KEYS_ONLY"
	KeyProjectionTypeINCLUDE  string = "INCLUDE"
	KeyProjectionTypeALL      string = "ALL"

	TagAttributeValue       string = "dynamodbav"
	TagAttributeType        string = "dynamodbat"
	TagPrimaryKey           string = "dynamodbpk"
	TagLocalSecondaryIndex  string = "dynamodblsi"
	TagGlobalSecondaryIndex string = "dynamodbgsi"
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

		if dynamodbavTagValue, ok := tp.Field(i).Tag.Lookup(TagAttributeValue); ok {
			attributeName = strings.Split(dynamodbavTagValue, ",")[0]
		} else {
			continue
		}

		if dynamodbpkTagValue, ok := tp.Field(i).Tag.Lookup(TagPrimaryKey); ok {
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

func (t *Table) localSecondaryIndexes() []*dynamodb.LocalSecondaryIndex {
	var (
		keys   []*dynamodb.LocalSecondaryIndex
		keyMap map[string]*dynamodb.LocalSecondaryIndex = make(map[string]*dynamodb.LocalSecondaryIndex)
		item   Itemer                                   = t.NewItem()
		tp     reflect.Type                             = reflect.TypeOf(item.GetItem()).Elem()
	)

	for i := 0; i < tp.NumField(); i++ {
		var (
			attributeName, keyType     string
			lsiName, lsiProjectionType string
			// lsiReadCapacityUnits, lsiWriteCapacityUnits int
		)

		if tp.Field(i).Anonymous && tp.Field(i).Type.Kind() != reflect.Struct {
			continue
		}

		if dynamodbavTagValue, ok := tp.Field(i).Tag.Lookup(TagAttributeValue); ok {
			attributeName = strings.Split(dynamodbavTagValue, ",")[0]
		} else {
			continue
		}

		if dynamodblsiTagValue, ok := tp.Field(i).Tag.Lookup(TagLocalSecondaryIndex); ok {
			var lsiType string

			lsiName, lsiType, _, _, lsiProjectionType = parseLsiTag(dynamodblsiTagValue)
			switch lsiType {
			case KeyTypeHASH, KeyTypeRANGE, KeyProjectionTypeINCLUDE:
				keyType = lsiType
				break
			default:
				continue
				break
			}

			// Make sure we have this index's key in the map
			if _, ok := keyMap[lsiName]; !ok {
				keyMap[lsiName] = &dynamodb.LocalSecondaryIndex{
					IndexName: aws.String(lsiName),
				}
			}

			// for HASH field only
			switch lsiProjectionType {
			case KeyProjectionTypeALL, KeyProjectionTypeINCLUDE, KeyProjectionTypeKEYSONLY:
				keyMap[lsiName].Projection = &dynamodb.Projection{
					ProjectionType: aws.String(lsiProjectionType),
				}
				break
			}

			// For INCLUDE fiels only
			if keyType == KeyProjectionTypeINCLUDE {
				if keyMap[lsiName].Projection == nil {
					keyMap[lsiName].Projection = &dynamodb.Projection{
						ProjectionType: aws.String(KeyProjectionTypeINCLUDE),
					}
				}

				keyMap[lsiName].Projection.NonKeyAttributes = append(keyMap[lsiName].Projection.NonKeyAttributes, aws.String(attributeName))

			} else {
				keyMap[lsiName].KeySchema = append(keyMap[lsiName].KeySchema, &dynamodb.KeySchemaElement{
					AttributeName: aws.String(attributeName),
					KeyType:       aws.String(keyType),
				})

			}

		} else {

			continue
		}

	}

	for _, v := range keyMap {
		keys = append(keys, v)
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
			skip                         bool = true
		)

		f := t.Field(i)
		if _, ok := f.Tag.Lookup(TagPrimaryKey); ok {
			skip = false
		}

		if tagValue, ok := f.Tag.Lookup(TagLocalSecondaryIndex); ok {
			if _, indexType, _, _, _ := parseLsiTag(tagValue); indexType == KeyProjectionTypeINCLUDE {
				skip = true
			} else {
				skip = false
			}
		}

		if skip {
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

	if dynamodbavTagValue, ok := f.Tag.Lookup(TagAttributeValue); ok {
		attributeName = strings.Split(dynamodbavTagValue, ",")[0]
	} else {
		return
	}

	if attributeTypeTagValue, ok := f.Tag.Lookup(TagAttributeType); ok {
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

// Examples
// 	`dynamodblsi:"IdDateLsi,HASH,3,3,ALL"`
//	`dynamodblsi:"IdDateLsi,RANGE"`
//	`dynamodblsi:"IdDateLsi,INCLUDE"`
func parseLsiTag(s string) (indexName, indexType string, indexRead, indexWrite int, indexProjectionType string) {
	slice := strings.Split(s, ",")

	if len(slice) >= 0 {
		indexName = slice[0]
	}

	if len(slice) >= 1 {
		switch slice[1] {
		case KeyTypeHASH, KeyTypeRANGE, KeyProjectionTypeINCLUDE:
			indexType = slice[1]
		}
	}

	if len(slice) >= 4 {
		v := slice[2]
		if i, err := strconv.Atoi(v); err != nil {
			indexRead = i
		}

		v = slice[3]
		if i, err := strconv.Atoi(v); err != nil {
			indexWrite = i
		}
	}

	if len(slice) == 5 {
		indexProjectionType = slice[5]
	}

	return
}
