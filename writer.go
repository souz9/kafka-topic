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
	ErrDropped = errors.New("too many messages in a queue, dropped")

	buffers = bpool.Pool{}
)

// Writer is a topic writer. Implements io.Writer.
type Writer struct {
	topic   string
	queue   chan *bpool.Buffer
	onError func(error)
}

// Write returns a new topic writer.
func Write(brokerNodes []string, topic string, writers, queueSize int) (*Writer, error) {
	w := &Writer{
		topic: topic,
		queue: make(chan *bpool.Buffer, writers+queueSize),
	}

	brokerConf := kafka.NewBrokerConf("kafka-topic")

	producerConf := kafka.NewProducerConf()
	producerConf.RetryLimit = 3
	producerConf.Compression = proto.CompressionSnappy
	producerConf.RequiredAcks = proto.RequiredAcksLocal

	for n := 0; n < writers; n++ {
		broker, err := kafka.Dial(brokerNodes, brokerConf)
		if err != nil {
			return nil, fmt.Errorf("kafka.Dial: %v", err)
		}
		producer := broker.Producer(producerConf)

		go w.writer(broker, producer)
	}
	return w, nil
}

// OnError sets the handler function that will be called if a error
// occurs while write to the broker.
func (w *Writer) OnError(handler func(error)) {
	w.onError = handler
}

// Write writes data to the topic.
// Actual writing to a broker happens asynchronously, see OnError method.
func (w *Writer) Write(data []byte) (int, error) {
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

func (w *Writer) writer(broker *kafka.Broker, producer kafka.Producer) {
	m := proto.Message{}

	for buf := range w.queue {
		// broker returns 0 for partition if an error occurs,
		// so the message will be written to there
		p, _ := broker.PartitionCount(w.topic)
		// Pick random partition as to provide write balancing
		if p > 0 {
			p = rand.Int31n(p)
		}

		m.Value = buf.B
		_, err := producer.Produce(w.topic, p, &m)
		if err != nil && w.onError != nil {
			w.onError(fmt.Errorf("kafka.Produce: %v", err))
		}

		buffers.Put(buf)
	}
}
