package writer

import (
	"context"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/database"
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

type Writer struct {
	dbInfo            *DatabaseInfo
	connector         *MongoDBConnector
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:            &DatabaseInfo{},
		connector:         NewMongoDBConnector(),
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
	}
}

func (writer *Writer) Init() error {

	// Connect to database
	err := writer.connector.Connect()
	if err != nil {
		return err
	}

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			writer.completionHandler(database.DBCommand(cmd))
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) ProcessData(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(reference, record, tables)
	}

	return nil

}

func (writer *Writer) InsertRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	// Getting collection
	mdb := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := mdb.Collection(record.Table)

	// Convert data to map
	doc := make(map[string]interface{}, len(record.Fields))
	for _, field := range record.Fields {
		doc[field.Name] = gravity_sdk_types_record.GetValue(field.Value)
	}

	// Write
	for {
		_, err := collection.InsertOne(context.Background(), doc)
		if err != nil {
			log.Error(err)
			<-time.After(time.Second * 5)
			log.WithFields(log.Fields{
				"event_name": record.EventName,
				"method":     record.Method.String(),
				"table":      record.Table,
			}).Warn("Retry to write record to database...")

			continue
		}
		break
	}

	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		Tables:    tables,
	}

	return nil
}

func (writer *Writer) UpdateRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	if record.PrimaryKey == "" {
		return nil
	}

	// Getting collection
	mdb := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := mdb.Collection(record.Table)

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
	for {
		_, err := collection.UpdateOne(
			context.Background(),
			bson.M{
				record.PrimaryKey: value,
			}, bson.M{
				"$set": doc,
			},
		)
		if err != nil {
			log.Error(err)
			<-time.After(time.Second * 5)
			log.WithFields(log.Fields{
				"event_name": record.EventName,
				"method":     record.Method.String(),
				"table":      record.Table,
			}).Warn("Retry to write record to database...")

			continue
		}
		break
	}

	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		Tables:    tables,
	}

	return nil
}

func (writer *Writer) DeleteRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	if record.PrimaryKey == "" {
		return nil
	}

	// Getting collection
	mdb := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))
	collection := mdb.Collection(record.Table)

	// Getting primary key
	var value interface{}
	for _, field := range record.Fields {
		if record.PrimaryKey == field.Name {
			value = gravity_sdk_types_record.GetValue(field.Value)
			break
		}
	}

	// Delete
	for {
		_, err := collection.DeleteOne(context.Background(), bson.M{
			record.PrimaryKey: value,
		})
		if err != nil {
			log.Error(err)
			<-time.After(time.Second * 5)
			log.WithFields(log.Fields{
				"event_name": record.EventName,
				"method":     record.Method.String(),
				"table":      record.Table,
			}).Warn("Retry to write record to database...")

			continue
		}
		break
	}

	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		Tables:    tables,
	}

	return nil
}
