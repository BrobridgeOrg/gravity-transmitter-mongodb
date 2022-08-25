module github.com/BrobridgeOrg/gravity-transmitter-mongodb

go 1.15

require (
	github.com/BrobridgeOrg/gravity-sdk v1.0.4
	github.com/cfsghost/buffered-input v0.0.2
	github.com/jinzhu/copier v0.3.2
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	go.mongodb.org/mongo-driver v1.10.1
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
