package main

import (
	"encoding/csv"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
)

type RedoLog struct {
	path      string           // 文件路径
	file      *os.File         // 文件句柄
	csvWriter *csv.Writer      // csv 封装
	reqChan   chan *logRequest // 请求管道
}

func NewRedoLog(logPath string) *RedoLog {

	err := os.MkdirAll(path.Dir(logPath), 0777)
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"logPath": logPath}).Fatal("[NewRedoLog] os.OpenFile() error.")
		return nil
	}

	l := &RedoLog{
		path:      logPath,
		file:      file,
		csvWriter: csv.NewWriter(file),
		reqChan:   make(chan *logRequest),
	}

	go l.loop()
	return l
}

func (l *RedoLog) Begin(tx *Transaction) {
	l.write(tx.Id, tx.State, tx.Op, tx.Key)
}

func (l *RedoLog) Prepare(tx *Transaction) {
	l.write(tx.Id, tx.State, tx.Op, tx.Key)
}

func (l *RedoLog) Abort(tx *Transaction) {
	l.write(tx.Id, tx.State, tx.Op, tx.Key)
}

func (l *RedoLog) Commit(tx *Transaction) {
	l.write(tx.Id, tx.State, tx.Op, tx.Key)
}

func (l *RedoLog) Write(tx *Transaction) {
	l.write(tx.Id, tx.State, tx.Op, tx.Key)
}

func (l *RedoLog) write(txId string, state TxState, op Operation, key string) {
	record := []string{txId, state.String(), op.String(), key}
	done := make(chan int)
	l.reqChan <- &logRequest{record, done}
	<-done
}

func (l *RedoLog) Read() (entries []Entry, err error) {
	entries = make([]Entry, 0)
	file, err := os.OpenFile(l.path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return
	}

	for _, record := range records {
		entries = append(entries, Entry{record[0], ParseTxState(record[1]), ParseOperation(record[2]), record[3]})
	}
	return
}

func (l *RedoLog) loop() {

	for {
		// 读取请求
		req := <-l.reqChan

		// 写文件
		err := l.csvWriter.Write(req.record)
		if err != nil {
			log.WithError(err).Fatalln("l.csvWriter.Write() fatal")
		}

		// flush
		l.csvWriter.Flush()

		// 磁盘 sync
		err = l.file.Sync()
		if err != nil {
			log.WithError(err).Fatalln("l.file.Sync() fatal")
		}

		// 回复
		req.done <- 1
	}
}
