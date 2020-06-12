package kfktool

import (
	"fmt"
	"log"
	"os"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Logger ...
type Logger struct {
	std             *log.Logger
	pInfo           *log.Logger
	pErr            *log.Logger
	cInfo           *log.Logger
	cErr            *log.Logger
	debug           bool
	onlyConsole     bool
	LogConsumerSucc bool
}

// PInfo publish info
func (l *Logger) PInfo(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[I] "+format, v...))
	}
	if l.onlyConsole {
		return
	}
	l.pInfo.Output(3, fmt.Sprintf("[I] "+format, v...))
}

// PErr publish error
func (l *Logger) PErr(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[E] "+format, v...))
	}
	if l.onlyConsole {
		return
	}
	l.pErr.Output(3, fmt.Sprintf("[E] "+format, v...))
}

// CInfo consumer info
func (l *Logger) CInfo(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[I] "+format, v...))
	}
	if l.onlyConsole {
		return
	}
	l.cInfo.Output(3, fmt.Sprintf("[I] "+format, v...))
}

// CErr consumer error
func (l *Logger) CErr(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[E] "+format, v...))
	}
	if l.onlyConsole {
		return
	}
	l.cErr.Output(3, fmt.Sprintf("[E] "+format, v...))
}

// NewLogger ...
func NewLogger(path, alias string, debug bool, args ...string) *Logger {
	logFlag := log.Ldate | log.Lmicroseconds | log.Lshortfile

	l := new(Logger)
	l.std = log.New(os.Stdout, "", logFlag)
	l.debug = debug

	if len(args) > 0 {
		l.onlyConsole = true
	}

	// info
	l.pInfo = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.producer.info.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I] ", logFlag)

	// err
	l.pErr = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.producer.error.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E] ", logFlag)

	l.cInfo = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.consumer.info.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I] ", logFlag)

	l.cErr = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/%s.consumer.error.log", path, alias),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E] ", logFlag)
	return l
}
