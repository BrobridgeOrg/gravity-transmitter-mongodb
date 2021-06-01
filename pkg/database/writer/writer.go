package writer

import (
	"context"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
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

func (writer *Writer) ProcessData(record *gravity_sdk_types_record.Record) error {

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
	}).Info("Write record")

	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(record)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(record)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(record)
	}

	return nil
}

func (writer *Writer) InsertRecord(record *gravity_sdk_types_record.Record) error {

	// Getting collection
	database := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := database.Collection(record.Table)

	// Convert data to map
	doc := make(map[string]interface{}, len(record.Fields))
	for _, field := range record.Fields {
		doc[field.Name] = gravity_sdk_types_record.GetValue(field.Value)
	}

	// Write
	_, err := collection.InsertOne(context.Background(), doc)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) UpdateRecord(record *gravity_sdk_types_record.Record) error {

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
			value = gravity_sdk_types_record.GetValue(field.Value)
			continue
		}

		// Getting updated fields
		doc[field.Name] = gravity_sdk_types_record.GetValue(field.Value)
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

func (writer *Writer) DeleteRecord(record *gravity_sdk_types_record.Record) error {

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
			value = gravity_sdk_types_record.GetValue(field.Value)
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
