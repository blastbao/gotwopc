package main

import (
	"errors"
	"fmt"
	"github.com/dchest/uniuri"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

type Master struct {
	Host string
	replicas []*ReplicaClient
	redoLog 		*RedoLog
	lockMgr 		*LockMgr
	txm              map[string]*Transaction
}

type Option struct {
	Port int
	LogPath  string // "logs/master.txt"
	Replicas []string
}

func NewMaster(opt *Option) *Master {
	host := fmt.Sprintf("%s:%d", "localhost", opt.Port)
	clients := make([]*ReplicaClient, 0, len(opt.Replicas))
	for _, replica := range opt.Replicas {
		client, err :=  NewReplicaClient(replica)
		if err != nil {
			log.Println("[NewMaster] err=", err)
		}
		clients = append(clients, client)
	}

	return &Master{
		Host: host,
		replicas: clients,
		txm: make(map[string]*Transaction),
		lockMgr: NewLockMgr(),
		redoLog: NewRedoLog(fmt.Sprintf("logs/master.txt")),
	}
}

func (m *Master) run() {

	if len(m.replicas) <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	err := m.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	log.Println("Master: listening on: ", m.Host)
	Run(m, m.Host)
}

func (m *Master) parseTx(e Entry) *Transaction {
	t := &Transaction{
		Id: e.Id,
		State: e.State,
		Key: e.Key,
		Value: e.Value,
		Op: e.Op,
		Redo: m.redoLog,
		logger: log.WithFields(log.Fields{"txId":e.Id,"Key":e.Key, "op":e.Op}),
	}
	return t
}

func (m *Master) recover() (err error) {

	entries, err := m.redoLog.Read()
	if err != nil {
		return
	}

	txs := make(map[string]*Transaction)
	for idx, entry := range entries {
		log.WithFields(log.Fields{"idx":idx,"txId": entry.Id,"Op":entry.Op,"Key":entry.Key,"State":entry.State}).Info("[recover] entry:")
		txs[entry.Id] = m.parseTx(entry)
	}

	for txId, tx := range txs {
		logger := log.WithFields(log.Fields{"tx":tx})
		logger.Info("[recover] transaction:")
		switch tx.State {
		case Aborted, Committed:
		case Started:
			logger.Info("transaction is `Started`, should m.sendAbort(txId).")
			m.sendAbort(txId)
		case Prepared:
			logger.Info("transaction is `Prepared`, should m.sendAbort(txId).")
			m.sendAbort(txId)
		}
	}

	return
}

func (m *Master) Get(args *GetArgs, reply *GetResult) (err error) {
	err = m.GetTest(&GetTestArgs{args.Key, -1}, reply)
	log.Println("[Get] receive request: ", args.Key, "response: ", reply.Value)
	return
}

func (m *Master) GetTest(args *GetTestArgs, reply *GetResult) (err error) {

	log.Println("[GetTest] receive request: ", args.Key, args.ReplicaNum)
	rn := args.ReplicaNum
	if rn < 0 {
		rn = rand.Intn(len(m.replicas))
	}

	r, err := m.replicas[rn].Get(args.Key)
	if err != nil {
		log.Printf("Master.Get: request to replica %v for key %v failed\n", rn, args.Key)
		return
	}
	reply.Value = *r
	return nil
}

func (m *Master) Del(args *DelArgs, _ *int) (err error) {
	return m.DelTest(&DelTestArgs{args.Key}, nil)
}

func (m *Master) DelTest(args *DelTestArgs, _ *int) (err error) {

	logger := log.WithFields(log.Fields{
		"key":args.Key,
	})

	tryDel := func(r *ReplicaClient, txId string) (bool, error) {
		return r.TryDel(args.Key, txId)
	}

	if err := m.try(DelOp, args.Key, "", tryDel); err != nil {
		logger.WithError(err).Error("[DelTest] m.try(PutOp, args.Key, tryDel) failed.")
		return err
	}
	logger.Info("[DelTest] ok.")
	return nil
}

func (m *Master) Put(args *PutArgs, _ *int) (err error) {
	return m.PutTest(&PutTestArgs{args.Key, args.Value}, nil)
}

func (m *Master) PutTest(args *PutTestArgs, _ *int) error {

	logger := log.WithFields(log.Fields{
		"key":args.Key,
		"value":args.Value,
	})

	tryPut := func(r *ReplicaClient, txId string) (bool, error) {
		return r.TryPut(args.Key, args.Value, txId)
	}

	if err := m.try(PutOp, args.Key,  args.Value, tryPut); err != nil {
		logger.WithError(err).Error("[PutTest] m.try(PutOp, args.Key, tryPut) failed.")
		return err
	}

	logger.Info("[PutTest] ok.")
	return nil
}

func (m *Master) try(op Operation, key, val string, call func(r *ReplicaClient, txId string) (bool, error)) (err error){

	logger := log.WithFields(log.Fields{
		"op":op,
		"key":key,
	})

	if m.lockMgr.Lock(key) {
		logger.Error("r.lockMgr.Lock(key) failed.")
		return errors.New("key is locked by others")
	}

	// 构造事务对象
	tx := &Transaction{
		Id: uniuri.New(),
		Op: op,
		Key: key,
		Value: val,
		//others
		Redo: m.redoLog,
		logger: log.WithFields(log.Fields{"Key":key, "op":op}),
	}
	m.txm[tx.Id] = tx

	// Start
	if err = tx.Start(); err != nil {
		logger.WithError(err).Error("[try] tx.Start() failed.")
		m.lockMgr.Unlock(key)
		return
	}

	// Prepare
	if err = tx.Prepare(); err != nil {
		logger.WithError(err).Fatal("[try] tx.Prepare() failed.")
		m.lockMgr.Unlock(key)
		return
	}

	// Prepare replicas
	err = m.sendPrepare(tx.Id, call)
	if err != nil {
		logger.WithError(err).Error("[try] m.sendPrepare(tx.Id, call) failed, should Abort().")
		if err = tx.Abort(); err != nil {
			logger.WithError(err).Fatal("[try] tx.Abort() failed.")
			return
		}
		m.sendAbort(tx.Id)
		m.lockMgr.Unlock(key)
		logger.Info("[try] tx.Abort() ok.")
		return TxAbortedError
	}
	logger.Info("[try] should commit.")

	// Commit
	if err = tx.Commit(); err != nil {
		logger.WithError(err).Fatal("[try] tx.Commit() failed.")
		return
	}

	// Commit replicas
	m.sendCommit(tx.Id)
	m.lockMgr.Unlock(key)

	logger.Info("[try] ok.")
	return
}

// 发送 "prepare" 给 replicas
func (m *Master) sendPrepare(txId string, call func(r *ReplicaClient, txId string) (bool, error)) error {
	logger := log.WithFields(log.Fields{"txId":txId})
	abort := make(chan int, len(m.replicas))
	m.forEachReplica(
		func(i int, r *ReplicaClient) {
			success, err := call(r, txId)
			if err != nil {
				logger.WithError(err).Error("[sendPrepare] call(r, tx.Id) error.")
				abort <- 1
				return
			}
			if !success {
				logger.WithFields(log.Fields{"success":success}).WithError(err).Error("[sendPrepare] call(r, tx.Id) failed.")
				abort <- 1
				return
			}
			logger.Info("[sendPrepare] call(r, tx.Id) ok.")
		},
	)

	select {
	// 失败，需要回滚
	case <-abort:
		logger.Error("[sendPrepare] should abort.")
		return TxAbortedError
	default:
		break
	}
	logger.Info("[sendPrepare] should commit.")
	return nil
}

// 发送 "abort" 给 replicas
func (m *Master) sendAbort(txId string) {
	logger := log.WithFields(log.Fields{"txId":txId})
	m.forEachReplica(func(i int, r *ReplicaClient) {
		for retry := 0; retry < 3; {
			success, err := r.Abort(txId)
			if err == nil {
				logger.WithField("success", success).Info("[sendAbort] r.Abort(txId) ok.")
				break
			}
			retry ++
			logger.WithField("retry", retry).WithError(err).Info("[sendAbort] r.Abort(txId) error, retry...")
			time.Sleep(100 * time.Millisecond)
		}
	})
}

func (m *Master) sendCommit(txId string) {
	logger := log.WithFields(log.Fields{"txId":txId})
	m.forEachReplica(func(i int, r *ReplicaClient) {
		for retry := 0; retry < 3; {
			success, err := r.Commit(txId)
			if err == nil {
				logger.WithField("success", success).Info("[sendCommit] r.Commit(txId) ok.")
				break
			}
			retry ++
			logger.WithField("retry", retry).WithError(err).Info("[sendCommit] r.Commit(txId) error, retry...")
			time.Sleep(100 * time.Millisecond)
		}
	})
}

// 并发调用 f() 的封装
func (m *Master) forEachReplica(f func(i int, r *ReplicaClient)) {
	var wg sync.WaitGroup
	wg.Add(len(m.replicas))
	for idx := range m.replicas {
		go func(i int, r *ReplicaClient) {
			defer wg.Done()
			f(i, r)
		}(idx, m.replicas[idx])
	}
	wg.Wait()
}

func (m *Master) Ping(args *PingArgs, reply *GetResult) (err error) {
	logger := log.WithFields(log.Fields{
		"key": args.Key,
	})
	reply.Value = args.Key
	logger.WithField("value", args.Key).Info("[Ping] ok")
	return nil
}

func (m *Master) Status(args *StatusArgs, reply *StatusResult) (err error) {
	logger := log.WithFields(log.Fields{
		"TxId": args.TxId,
	})

	reply.State = InValid
	tx, ok := m.txm[args.TxId]
	if ok {
		reply.State = tx.State
	}
	logger.Info("[Status] ok.")
	return nil
}
