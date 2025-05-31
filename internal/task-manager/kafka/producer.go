package kafka

import (
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)
const (
	DefaultKafkaBrokers      = "localhost:9092"
	DefaultTaskDispatchTopic = "task_execution_requests"
)
func NewKafkaProducer() *kafka.Writer {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS"); if kafkaBrokers == "" { kafkaBrokers = DefaultKafkaBrokers }
	taskDispatchTopic := os.Getenv("TASK_DISPATCH_TOPIC"); if taskDispatchTopic == "" { taskDispatchTopic = DefaultTaskDispatchTopic }
	brokerList := strings.Split(kafkaBrokers, ",")
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokerList,
		Topic:        taskDispatchTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireOne), // Cast to int as last resort
		Async:        false,
	})
	log.Printf("Task Manager Kafka producer configured for topic: %s", taskDispatchTopic)
	return producer
}
