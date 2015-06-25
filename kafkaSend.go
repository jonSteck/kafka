package kafka

import (
	"gopkg.in/Shopify/sarama.v1"
	"fmt"
	"log"
	"net/http"
)

//create struct to hold kafka syncronous producer and functions
type Server struct {
        DataCollector           sarama.SyncProducer
}

//must close server - will not be garbage collected
func (s *Server) Close() error {
        if err := s.DataCollector.Close(); err != nil {
                log.Println("Failed to shut down data collector", err)
        }
        return nil
}

//send string value to kafka topic
func (s *Server) KafkaSend(w http.ResponseWriter, topic string, value string) {
	partition, offset, err := s.DataCollector.SendMessage(&sarama.ProducerMessage {
		Topic: topic,
		Value: sarama.StringEncoder(value),
	})

	if err != nil {
		fmt.Fprintf(w, "Failed to store your data %s", err)
	} else {
		fmt.Fprintf(w, "Your data is stored in Kafka with id %s/%d/%d (topic/partition/offset)", topic, partition, offset)
	}
}

//send encoded avro []byte to kafka topic
func (s *Server) KafkaSendAvro(w http.ResponseWriter, topic string, value []byte) {
        partition, offset, err := s.DataCollector.SendMessage(&sarama.ProducerMessage {
                Topic: topic,
                Value: sarama.ByteEncoder(value),
        })

        if err != nil {
                fmt.Fprintf(w, "Failed to store your data %s", err)
        } else {
                fmt.Fprintf(w, "Your data is stored in Kafka with id %s/%d/%d (topic/partition/offset)", topic, partition, offset)
        }
}

//initialize kafka syncronous producer
func NewDataCollector(brokerList []string) sarama.SyncProducer {
        config := sarama.NewConfig()
        producer, err := sarama.NewSyncProducer(brokerList, config)
        if err != nil {
                log.Fatalln("Failed to start Sarama producer", err)
        }

        return producer
}

