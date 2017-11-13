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
	"gopkg.in/mgo.v2/bson"
)

var (
	url *string
)

type posConnection struct {
	db *mgo.Database
}

func getCloudAmqpURL() (url *string) {
	amqpUser := os.Getenv("CLOUD_AMQP_USER")
	missingEnv := false

	if amqpUser == "" {
		missingEnv = true
		log.Printf("$CLOUD_AMQP_USER must be set")
	}

	amqpPassword := os.Getenv("CLOUD_AMQP_PASSWORD")

	if amqpPassword == "" {
		missingEnv = true
		log.Printf("$CLOUD_AMQP_PASSWORD must be set")
	}

	if missingEnv {
		panic("CloudAmqp environment variables not configured")
	}

	url = flag.String("url", fmt.Sprintf("amqp://%s:%s@elephant.rmq.cloudamqp.com/%s", amqpUser, amqpPassword, amqpUser), "amqp url")
	return url
}

func showUsageAndStatus() {
	fmt.Printf("Consumer is running\n\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
}

func main() {

	url = getCloudAmqpURL()
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
		//cony.AutoAck(), // Auto sign the deliveries
	)
	cli.Consume(cns)
	for cli.Loop() {
		select {
		case msg := <-cns.Deliveries():
			log.Printf("Received body: %q\n", msg.Body)

			// starts a new `goroutine` to process the msg.
			go handleMessage(&msg, db)

		case err := <-cns.Errors():
			fmt.Printf("Consumer error: %v\n", err)
		case err := <-cli.Errors():
			fmt.Printf("Client error: %v\n", err)
		}
	}
}

// handleMessage process a new purchase message
func handleMessage(msg *amqp.Delivery, conn *posConnection) error {
	// reuse from connection pool
	session := conn.db.Session.Copy()
	defer session.Close()

	// defines an object with Purchase model defined in POS API
	payload := model.Purchase{}
	payload.Status = "FINISHED"

	// deserializes the payload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		fmt.Printf("Consumer failed to deserialize payload: %v\n", err)
		msg.Nack(false, true) //failed to deserialize, requeues the message
		return err
	}

	collection := session.DB("services-pos").C("Purchase")

	err := collection.Update(bson.M{"_id": payload.TransactionID}, bson.M{"$set": bson.M{"status": payload.Status}})

	if err != nil {
		fmt.Printf("Consumer failed to update Purchase (ID=%q) status: %v\n", payload.TransactionID, err)
		msg.Nack(false, true)
		return err
	}

	// If when we built the consumer we didn't use
	// the "cony.AutoAck()" option this is where we'd
	// have to call the "amqp.Deliveries" methods "Ack",
	// "Nack", "Reject"
	//
	// msg.Ack(false)
	// msg.Nack(false)
	// msg.Reject(false)
	return msg.Ack(false)
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
