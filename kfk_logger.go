package kfktool

import (
	"fmt"
	"log"
	"os"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Logger ...
type Logger struct {
	std   *log.Logger
	pInfo *log.Logger
	pErr  *log.Logger
	debug bool
	newer bool
}

// PInfo publish info
func (l *Logger) PInfo(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[I] "+format, v...))
	}
	l.pInfo.Output(3, fmt.Sprintf("[I] "+format, v...))
}

// PErr publish error
func (l *Logger) PErr(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf("[E] "+format, v...))
	}
	l.pErr.Output(3, fmt.Sprintf("[E] "+format, v...))
}

// Ptrace publish default trace
func (l *Logger) Ptrace(format string, v ...interface{}) {
	if l.debug {
		l.std.Output(3, fmt.Sprintf(format, v...))
	}
}

// NewLogger ...
func NewLogger(path, alias string, flag ...int) *Logger {
	logFlag := log.Ldate | log.Lmicroseconds | log.Lshortfile

	l := new(Logger)
	l.std = log.New(os.Stdout, "", logFlag)

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

	return l
}
