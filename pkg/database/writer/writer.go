package writer

import (
	"context"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-mongodb/pkg/database"

	buffered_input "github.com/cfsghost/buffered-input"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

type CollectionRecord struct {
	models []mongo.WriteModel
	cmds   []*DBCommand
}

type Writer struct {
	dbInfo            *DatabaseInfo
	connector         *MongoDBConnector
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
	buffer            *buffered_input.BufferedInput
}

func NewWriter() *Writer {

	writer := &Writer{
		dbInfo:            &DatabaseInfo{},
		connector:         NewMongoDBConnector(),
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
	}
	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = viper.GetInt("bufferInput.chunkSize")
	opts.ChunkCount = 10000
	opts.Timeout = viper.GetDuration("bufferInput.timeout") * time.Millisecond
	opts.Handler = writer.chunkHandler
	writer.buffer = buffered_input.NewBufferedInput(opts)

	return writer
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
			writer.buffer.Push(cmd)
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) chunkHandler(chunk []interface{}) {

	dbCommands := make([]*DBCommand, 0, len(chunk))
	for _, request := range chunk {
		req := request.(*DBCommand)
		dbCommands = append(dbCommands, req)
	}
	writer.processData(dbCommands)
}

func (writer *Writer) processData(dbCommands []*DBCommand) {

	// Getting collection
	mdb := writer.connector.GetClient().Database(viper.GetString("mongodb.dbname"))

	colls := make(map[string]*CollectionRecord, 0)
	//var models []mongo.WriteModel
	//var cmds []*DBCommand
	for _, cmd := range dbCommands {

		record := cmd.Record
		// Convert data to map
		doc := make(map[string]interface{}, len(record.Fields))

		var model mongo.WriteModel

		switch record.Method {
		case gravity_sdk_types_record.Method_DELETE:
			var value interface{}
			for _, field := range record.Fields {
				// Getting primary key
				if record.PrimaryKey == field.Name {
					value = gravity_sdk_types_record.GetValue(field.Value)
					continue
				}
			}
			model = mongo.NewDeleteOneModel().SetFilter(bson.M{record.PrimaryKey: value})

		case gravity_sdk_types_record.Method_UPDATE:
			var value interface{}
			for _, field := range record.Fields {
				// Getting primary key
				if record.PrimaryKey == field.Name {
					value = gravity_sdk_types_record.GetValue(field.Value)
					continue
				}

				// Getting updated fields
				doc[field.Name] = gravity_sdk_types_record.GetValue(field.Value)
			}

			model = mongo.NewUpdateOneModel().SetFilter(bson.M{record.PrimaryKey: value}).SetUpdate(bson.M{"$set": doc})

		case gravity_sdk_types_record.Method_INSERT:
			for _, field := range record.Fields {
				doc[field.Name] = gravity_sdk_types_record.GetValue(field.Value)
			}

			model = mongo.NewInsertOneModel().SetDocument(doc)

		}

		// Getting status for specific table
		collectionRecord, ok := colls[cmd.Record.Table]
		if !ok {
			collectionRecord = &CollectionRecord{}
			colls[cmd.Record.Table] = collectionRecord
		}

		// Update models and commands
		collectionRecord.models = append(collectionRecord.models, model)
		collectionRecord.cmds = append(collectionRecord.cmds, cmd)
	}

	opts := options.BulkWrite().SetOrdered(false)

	// Perform updates for each table
	for table, colRecord := range colls {

		collection := mdb.Collection(table)
		cmds := colRecord.cmds
		models := colRecord.models

		for {

			result, err := collection.BulkWrite(context.Background(), models, opts)

			// Commands was finished
			total := result.InsertedCount + result.ModifiedCount + result.DeletedCount
			for _, cmd := range cmds[:total] {
				writer.completionHandler(cmd)
			}

			// Update cursor
			cmds = cmds[total:]
			models = models[total:]

			// Perform the rest of updates in 3 seconds
			if err != nil {
				log.Error(err)
				time.Sleep(3 * time.Second)
				continue
			}

			break
		}
	}

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

	writer.commands <- &DBCommand{
		Reference: reference,
		Record:    record,
		Tables:    tables,
	}

	return nil
}
