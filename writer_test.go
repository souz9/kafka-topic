package ktopic

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

var brokerNodes = []string{"127.0.0.1:9092"}

type topicReader struct{ r *kafka.Reader }

func readTopic(t *testing.T, brokerNodes []string, topic string) topicReader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokerNodes,
		Topic:     topic,
		Partition: 0,
		MaxWait:   100 * time.Millisecond,
	})

	err := reader.SetOffset(kafka.LastOffset)
	require.NoError(t, err)

	return topicReader{r: reader}
}

func (r topicReader) Read(t *testing.T) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m, err := r.r.ReadMessage(ctx)
	require.NoError(t, err)

	return m.Value
}

func TestWriter(t *testing.T) {
	const queueSize = 10

	r := readTopic(t, brokerNodes, "test")

	w := Write(brokerNodes, "test", 1, queueSize)
	w.OnError(func(err error) {
		assert.NoError(t, err)
	})

	t.Run("should write messages", func(t *testing.T) {
		ts := time.Now().UnixNano()
		a := strconv.FormatInt(ts, 10)
		b := strconv.FormatInt(ts+1, 10)

		w.Write([]byte(a))
		w.Write([]byte(b))
		assert.Equal(t, []byte(a), r.Read(t))
		assert.Equal(t, []byte(b), r.Read(t))
	})

	t.Run("should drop messages if write too quickly", func(t *testing.T) {
		for i := 0; i < 2*queueSize; i++ {
			w.Write([]byte("data"))
		}
		n, err := w.Write([]byte("data"))
		assert.Equal(t, ErrDropped, err)
		assert.Equal(t, 0, n)
	})
}
