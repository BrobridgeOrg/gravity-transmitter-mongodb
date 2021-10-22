module github.com/BrobridgeOrg/gravity-transmitter-mongodb

go 1.15

require (
	github.com/BrobridgeOrg/gravity-sdk v0.0.47
	github.com/jinzhu/copier v0.3.2
	github.com/jmoiron/sqlx v1.3.4
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	go.mongodb.org/mongo-driver v1.5.2
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
