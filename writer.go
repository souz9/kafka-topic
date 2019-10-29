package ktopic

import (
	"context"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
	"github.com/souz9/bpool"
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
func Write(brokerNodes []string, topic string, writers, queueSize int) *Writer {
	w := &Writer{
		topic: topic,
		queue: make(chan *bpool.Buffer, writers+queueSize),
	}

	producerConfig := kafka.WriterConfig{
		Brokers:          brokerNodes,
		Topic:            topic,
		RequiredAcks:     1,
		BatchSize:        1,
		BatchBytes:       100 << 20, // 100 MB
		CompressionCodec: snappy.NewCompressionCodec(),
	}

	for n := 0; n < writers; n++ {
		producer := kafka.NewWriter(producerConfig)
		go w.writer(producer)
	}
	return w
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
		MetricQueued.WithLabelValues(w.topic).Inc()
		return len(data), nil
	default:
		MetricDropped.WithLabelValues(w.topic).Inc()
		return 0, ErrDropped
	}
}

func (w *Writer) writer(producer *kafka.Writer) {
	for buf := range w.queue {
		t := prometheus.NewTimer(MetricWriteDuration.WithLabelValues(w.topic))
		err := producer.WriteMessages(
			context.Background(), kafka.Message{Value: buf.B})
		t.ObserveDuration()

		if err != nil && w.onError != nil {
			w.onError(fmt.Errorf("produce: %v", err))
		}

		buffers.Put(buf)
		MetricQueued.WithLabelValues(w.topic).Dec()
	}
}
