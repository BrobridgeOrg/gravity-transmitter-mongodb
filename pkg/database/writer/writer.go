package writer

import (
	"context"
	"encoding/binary"
	"math"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	UpdateTemplate = `UPDATE "%s" SET %s WHERE "%s" = :primary_val`
	InsertTemplate = `INSERT INTO "%s" (%s) VALUES (%s)`
	DeleteTemplate = `DELETE FROM "%s" WHERE "%s" = :primary_val`
)

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Secure   bool   `json:"secure"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
}

type Writer struct {
	dbInfo    *DatabaseInfo
	connector *MongoDBConnector
	//	db       *sqlx.DB
	commands chan *DBCommand
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:    &DatabaseInfo{},
		connector: NewMongoDBConnector(),
		commands:  make(chan *DBCommand, 2048),
	}
}

func (writer *Writer) Init() error {

	// Connect to database
	err := writer.connector.Connect()
	if err != nil {
		return err
	}

	//TODO: Reconnect

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case _ = <-writer.commands:
			/*
				_, err := writer.db.NamedExec(cmd.QueryStr, cmd.Args)
				if err != nil {
					log.Error(err)
				}
			*/
		}
	}
}

func (writer *Writer) ProcessData(record *transmitter.Record) error {

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
	}).Info("Write record")

	switch record.Method {
	case transmitter.Method_DELETE:
		return writer.DeleteRecord(record)
	case transmitter.Method_UPDATE:
		return writer.UpdateRecord(record)
	case transmitter.Method_INSERT:
		return writer.InsertRecord(record)
	}

	return nil
}

func (writer *Writer) GetValue(value *transmitter.Value) interface{} {

	switch value.Type {
	case transmitter.DataType_FLOAT64:
		return math.Float64frombits(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_INT64:
		return int64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_UINT64:
		return uint64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_BOOLEAN:
		return int8(value.Value[0]) & 1
	case transmitter.DataType_STRING:
		return string(value.Value)
	case transmitter.DataType_MAP:
		mapValue := make(map[string]interface{}, len(value.Map.Fields))
		for _, field := range value.Map.Fields {
			mapValue[field.Name] = writer.GetValue(field.Value)
		}
		return mapValue
	case transmitter.DataType_ARRAY:
		arrayValue := make([]interface{}, len(value.Array.Elements))
		for _, ele := range value.Array.Elements {
			v := writer.GetValue(ele)
			arrayValue = append(arrayValue, v)
		}
		return arrayValue
	}

	// binary
	return value.Value
}

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	// Getting collection
	database := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := database.Collection(record.Table)

	// Convert data to map
	doc := make(map[string]interface{}, len(record.Fields))
	for _, field := range record.Fields {
		doc[field.Name] = writer.GetValue(field.Value)
	}

	// Write
	_, err := collection.InsertOne(context.Background(), doc)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	if record.PrimaryKey == "" {
		return nil
	}

	// Getting collection
	database := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := database.Collection(record.Table)

	var value interface{}
	doc := make(map[string]interface{}, len(record.Fields))
	for _, field := range record.Fields {

		// Getting primary key
		if record.PrimaryKey == field.Name {
			value = writer.GetValue(field.Value)
			continue
		}

		// Getting updated fields
		doc[field.Name] = writer.GetValue(field.Value)
	}

	// Update
	_, err := collection.UpdateOne(
		context.Background(),
		bson.M{
			record.PrimaryKey: value,
		}, bson.M{
			"$set": doc,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	if record.PrimaryKey == "" {
		return nil
	}

	// Getting collection
	database := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := database.Collection(record.Table)

	// Getting primary key
	var value interface{}
	for _, field := range record.Fields {
		if record.PrimaryKey == field.Name {
			value = writer.GetValue(field.Value)
			break
		}
	}

	// Write
	_, err := collection.DeleteOne(context.Background(), bson.M{
		record.PrimaryKey: value,
	})
	if err != nil {
		return err
	}

	return nil
}
