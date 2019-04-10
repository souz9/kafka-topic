package ktopic

import (
	"errors"
	"fmt"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
	"github.com/souz9/bpool"
	"math/rand"
)

var (
	ErrDropped = errors.New("message was dropped")

	buffers = bpool.Pool{}
)

// TopicWriter is a topic writer. Implements io.Writer.
type TopicWriter struct {
	queue chan<- *bpool.Buffer
}

// WriteTopic returns a new topic writer.
func (nodes BrokerNodes) WriteTopic(topic string, writers, queueSize int) (*TopicWriter, error) {
	brokerConf := kafka.NewBrokerConf("kafka-topic-writer")

	producerConf := kafka.NewProducerConf()
	producerConf.RetryLimit = 3
	producerConf.Compression = proto.CompressionSnappy
	producerConf.RequiredAcks = proto.RequiredAcksLocal

	queue := make(chan *bpool.Buffer, writers+queueSize)

	for n := 0; n < writers; n++ {
		broker, err := kafka.Dial(nodes, brokerConf)
		if err != nil {
			return nil, fmt.Errorf("kafka.Dial(%v): %v", nodes, err)
		}
		producer := broker.Producer(producerConf)

		go writer(broker, producer, topic, queue)
	}
	return &TopicWriter{queue}, nil
}

// Write writes data to the topic.
func (w *TopicWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// We need a copy of data as it will be written async
	buf := buffers.Get(len(data))
	buf.B = buf.B[:len(data)]
	copy(buf.B, data)

	select {
	case w.queue <- buf:
		return len(data), nil
	default:
		return 0, ErrDropped
	}
}

func writer(
	broker *kafka.Broker,
	producer kafka.Producer,
	topic string,
	queue <-chan *bpool.Buffer,
) {
	m := proto.Message{}

	for buf := range queue {
		// broker returns 0 for partition if an error occurs,
		// so the message will be written to there
		p, _ := broker.PartitionCount(topic)
		// Pick random partition as to provide write balancing
		if p > 0 {
			p = rand.Int31n(p)
		}

		m.Value = buf.B
		if _, err := producer.Produce(topic, p, &m); err != nil {
			Logger.Errorf("kafka.Produce(%v): %v", topic, err)
		}

		buffers.Put(buf)
	}
}
