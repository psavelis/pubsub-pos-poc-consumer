package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/assembla/cony"
	model "github.com/psavelis/goa-pos-poc/app"
	"github.com/streadway/amqp"
	mgo "gopkg.in/mgo.v2"
)

var url = flag.String("url", "amqp://usr:pwd@elephant.rmq.cloudamqp.com/rwpvuvoo", "amqp url")

type posConnection struct {
	db *mgo.Database
}

func showUsageAndStatus() {
	fmt.Printf("Consumer is running\n\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
}

func main() {

	flag.Parse()

	showUsageAndStatus()

	// Creates a mongodb database instance
	db := getDatabase()

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URL(*url),
		cony.Backoff(cony.DefaultBackoff),
	)

	// Declarations
	// The queue name will be supplied by the AMQP server
	que := &cony.Queue{
		AutoDelete: true,
		Name:       "pos-purchase-created-queue",
	}
	exc := cony.Exchange{
		Name:       "purchase.created",
		Kind:       "fanout",
		AutoDelete: true,
	}
	bnd := cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      "pubSub",
	}
	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})

	// Declare and register a consumer
	cns := cony.NewConsumer(
		que,
		cony.AutoAck(), // Auto sign the deliveries
	)
	cli.Consume(cns)
	for cli.Loop() {
		select {
		case msg := <-cns.Deliveries():
			log.Printf("Received body: %q\n", msg.Body)
			// If when we built the consumer we didn't use
			// the "cony.AutoAck()" option this is where we'd
			// have to call the "amqp.Deliveries" methods "Ack",
			// "Nack", "Reject"
			//
			// msg.Ack(false)
			// msg.Nack(false)
			// msg.Reject(false)
			handleMessage(&msg, db)
		case err := <-cns.Errors():
			fmt.Printf("Consumer error: %v\n", err)
		case err := <-cli.Errors():
			fmt.Printf("Client error: %v\n", err)
		}
	}
}

func handleMessage(msg *amqp.Delivery, conn *posConnection) {
	// reuse from connection pool
	session := conn.db.Session.Copy()
	defer session.Close()

	// defines an object with Purchase model defined in POS API
	payload := model.Purchase{}

	// deserializes the payload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		msg.Nack(false, true) //failed to deserialize, requeues the message
	}

	// TODO: process the new purchase and update status to `FINISHED``

}

func getDatabase() (db *posConnection) {
	// MongoDB (Atlas) setup
	tlsConfig := &tls.Config{}
	tlsConfig.InsecureSkipVerify = true

	mgoUser := os.Getenv("MONGO_USR")

	if mgoUser == "" {
		log.Printf("$MONGO_USR must be set")
	}

	mgoPassword := os.Getenv("MONGO_PWD")

	if mgoPassword == "" {
		log.Printf("$MONGO_PWD must be set")
	}

	dialInfo, err := mgo.ParseURL(fmt.Sprintf("mongodb://%s:%s@development-shard-00-00-ozch3.mongodb.net:27017,development-shard-00-01-ozch3.mongodb.net:27017,development-shard-00-02-ozch3.mongodb.net:27017/test?replicaSet=development-shard-0&authSource=admin", mgoUser, mgoPassword))

	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	// services-pos database
	database := *session.DB("services-pos")

	return &posConnection{db: &database}
}
