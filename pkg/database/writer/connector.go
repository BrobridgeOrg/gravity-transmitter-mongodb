package writer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBConnector struct {
	client *mongo.Client
}

func NewMongoDBConnector() *MongoDBConnector {
	return &MongoDBConnector{}
}

func (mdb *MongoDBConnector) Connect() error {

	uri := viper.GetString("mongodb.uri")

	log.WithFields(log.Fields{
		"uri": uri,
	}).Info("Connect to MongoDB")

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Load CA file
	caFile := viper.GetString("mongodb.ca_file")
	if len(caFile) > 0 {
		tlsConfig, err := mdb.LoadCert(caFile)
		if err != nil {
			return err
		}

		clientOptions.SetTLSConfig(tlsConfig)
	}

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return err
	}

	mdb.client = client

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	log.Info("Connected to MongoDB Successfully")

	// Initializing database
	return mdb.InitializeDatabase()
}

func (mdb *MongoDBConnector) LoadCert(caFile string) (*tls.Config, error) {

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (mdb *MongoDBConnector) InitializeDatabase() error {

	return nil
}

func (mdb *MongoDBConnector) GetClient() *mongo.Client {
	return mdb.client
}
