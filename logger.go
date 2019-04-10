package ktopic

var Logger interface {
	Errorf(format string, v ...interface{})
} = nullLogger{}

type nullLogger struct{}

func (l nullLogger) Errorf(format string, v ...interface{}) {}
