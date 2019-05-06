package rabbitMQ

import (
	"github.com/streadway/amqp"
	"log"
	"os"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type callback func(string)

func StartRabbit(callback callback) {
	log.Println("RabbitMQ listener started")

	conn, err := amqp.Dial(os.Getenv("RABBIT_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	notify := conn.NotifyClose(make(chan *amqp.Error))

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("RABBIT_QUEUE_NAME"), // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	for {
		select {
		case err := <-notify:
			log.Fatal(err)
			break
		case d := <-msgs:
			log.Println(d.Body)
		}
	}
}
