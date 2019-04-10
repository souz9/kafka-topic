package ktopic

import (
	"github.com/optiopay/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
	"time"
)

var brokerNodes = BrokerNodes{"127.0.0.1:9092"}

type topicReader struct{ kafka.Consumer }

func (nodes BrokerNodes) readTopic(t *testing.T, topic string) topicReader {
	broker, err := kafka.Dial(nodes, kafka.NewBrokerConf("kafka"))
	require.NoError(t, err)

	conf := kafka.NewConsumerConf(topic, 0)
	conf.StartOffset = kafka.StartOffsetNewest
	conf.RetryLimit = 1
	cons, err := broker.Consumer(conf)
	require.NoError(t, err)

	return topicReader{cons}
}

func (r topicReader) ReadWait(t *testing.T) []byte {
	start := time.Now()
	for time.Since(start) < time.Second {
		m, err := r.Consume()
		if err == kafka.ErrNoData {
			runtime.Gosched()
			continue
		}
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		return m.Value
	}
	assert.FailNow(t, "timeout")
	return nil
}

func TestWriter(t *testing.T) {
	const queueSize = 10

	w, err := brokerNodes.WriteTopic("test", 1, queueSize)
	require.NoError(t, err)

	r := brokerNodes.readTopic(t, "test")

	t.Run("should write messages", func(t *testing.T) {
		w.Write([]byte("one"))
		w.Write([]byte("two"))
		assert.Equal(t, []byte("one"), r.ReadWait(t))
		assert.Equal(t, []byte("two"), r.ReadWait(t))
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
