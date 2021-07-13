module github.com/BrobridgeOrg/gravity-transmitter-mongodb

go 1.15

require (
	github.com/BrobridgeOrg/gravity-sdk v0.0.25
	github.com/jinzhu/copier v0.3.2
	github.com/jmoiron/sqlx v1.2.0
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
	go.mongodb.org/mongo-driver v1.5.2
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
