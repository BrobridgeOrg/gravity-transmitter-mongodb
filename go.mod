module github.com/BrobridgeOrg/gravity-transmitter-mongodb

go 1.15

require (
	github.com/BrobridgeOrg/gravity-api v0.2.4
	github.com/BrobridgeOrg/gravity-transmitter-postgres v0.0.0-20201002211924-6b3def7a7db0
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.8.0
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.7.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	go.mongodb.org/mongo-driver v1.4.4
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	google.golang.org/grpc v1.31.0
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
