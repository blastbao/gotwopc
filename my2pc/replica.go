package main

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"

	"strings"
	"time"
)

func init(){
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
}

type Replica struct {
	Host string
	ID            int
	redoLog 		*RedoLog
	lockMgr 		*LockMgr
	txm              map[string]*Transaction
	store            *MemStore
	temp             *MemStore
}

func (r *Replica) run() {
	err := r.recover()
	if err != nil {
		log.WithError(err).Fatal("[run] r.recover() failed.")
	}
	log.WithError(err).WithFields(log.Fields{"replicaNum":r.ID, "host":r.Host}).Info("[run] ok.")
	Run(r, r.Host)
}

func NewReplica(id int) *Replica {
	return &Replica{
		ID: id,
		Host:GetReplicaHost(id),
		redoLog: NewRedoLog(fmt.Sprintf("logs/replica%v.txt", id)),
		lockMgr: NewLockMgr(),
		txm: make(map[string]*Transaction),
		store: NewMemStore(),
		temp: NewMemStore(),
	}
}

func (r *Replica) getTempKey(txId string, key string) string {
	return txId + "__" + key
}

func (r *Replica) parseTempKey(key string) (txId string, txKey string) {
	split := strings.Split(key, "__")
	return split[0], split[1]
}

func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) error {

	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"Key":args.Key,
		"Value": args.Value,
	})

	writeTemp := func() error {
		r.temp.Put(r.getTempKey(args.TxId, args.Key), args.Value)
		return nil
	}

	ok, err := r.try(args.TxId, args.Key, PutOp, writeTemp)
	if err != nil {
		logger.WithError(err).Error("[TryPut] failed.")
		return err
	}

	reply.Success = ok
	logger.WithField("ok",  ok).Info("[TryPut] ok")
	return nil
}

func (r *Replica) TryDel(args *TxDelArgs, reply *ReplicaActionResult) error {

	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"Key":args.Key,
	})

	ok, err := r.try(args.TxId, args.Key, DelOp, nil)
	if err != nil {
		logger.WithError(err).Error("[TryDel] failed.")
		return err
	}
	reply.Success = ok
	logger.WithField("ok", ok).Info("[TryDel] ok")
	return nil
}

func (r *Replica) try(txId string, key string, op Operation, f func() error) (ok bool, err error) {

	logger := log.WithFields(log.Fields{
		"txId":txId,
		"Key":key,
		"op":op,
	})

	ok = false

	// 构造事务对象
	tx := &Transaction{
		// core
		Id: txId,
		Key: key,
		Op: op,
		State: Started,
		F: f,

		//others
		Redo: r.redoLog,
		Lock: r.lockMgr,
		logger: log.WithFields(log.Fields{"txId":txId,"Key":key, "op":op}),
	}
	r.txm[txId] = tx

	if err = tx.Start(); err != nil {
		logger.WithError(err).Error("[try] tx.Start() failed.")
		return
	}

	if err = tx.Prepare(); err != nil {
		logger.WithError(err).Error("[try] tx.Prepare() failed.")
		return
	}

	ok = true
	logger.WithField("ok", ok).Info("[try] ok.")
	return
}

func (r *Replica) Commit(args *CommitArgs, reply *ReplicaActionResult) error {

	logger := log.WithFields(log.Fields{
		"txId": args.TxId,
	})

	reply.Success = false

	tx, ok := r.txm[args.TxId]
	if !ok {
		logger.Error("[TCommit] tx not exist.")
		return errors.New("tx not exist")
	}

	if tx.State != Prepared {
		logger.WithField("state", tx.State).Error("[TCommit] tx.State != Prepared.")
		return fmt.Errorf("can not commit, tx.State=%s", tx.State)
	}

	if err := r.commit(tx); err != nil {
		logger.WithError(err).Error("[TCommit] r.tCommit(tx) failed.")
		return err
	}

	reply.Success = true
	return nil
}

func (r *Replica) commit(tx *Transaction) (err error) {

	tempKey := r.getTempKey(tx.Id, tx.Key)

	switch tx.Op {
	case PutOp:
		val, ok := r.temp.Get(tempKey)
		if !ok {
			return errors.New("r.temp.Get(key) failed, not exist")
		}
		r.store.Put(tx.Key, val)
	case DelOp:
		r.temp.Del(tempKey)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// 在提交后才会删除临时数据，以防止我们在删除后、提交前崩溃。
	if tx.Op == PutOp {
		r.temp.Del(tempKey)
	}

	return nil
}

func (r *Replica) abort(tx *Transaction) (err error) {

	tempKey := r.getTempKey(tx.Id, tx.Key)

	switch tx.Op {
	case PutOp:
		r.temp.Del(tempKey)
	//case DelOp:
		// do nothing
	}
	if err := tx.Abort(); err != nil {
		return err
	}
	// 移除
	delete(r.txm, tx.Id)
	return nil
}

func (r *Replica) Abort(args *AbortArgs, reply *ReplicaActionResult) (err error) {

	logger := log.WithFields(log.Fields{
		"txId": args.TxId,
	})

	reply.Success = false

	tx, ok := r.txm[args.TxId]
	if !ok {
		logger.Error("[TAbort] tx not exist.")
		return errors.New("tx not exist")
	}

	if tx.State != Prepared {
		logger.WithField("state", tx.State).Error("[TAbort] tx.State != Prepared.")
		return fmt.Errorf("can not commit, tx.State=%s", tx.State)
	}

	if err := r.abort(tx); err != nil {
		return err
	}

	reply.Success = true
	return nil
}

func (r *Replica) parseTx(e Entry) *Transaction {
	t := &Transaction{
		Id: e.Id,
		State: e.State,
		Key: e.Key,
		Op: e.Op,
		Lock: r.lockMgr,
		Redo: r.redoLog,
		logger: log.WithFields(log.Fields{"txId":e.Id,"Key":e.Key, "op":e.Op}),
	}
	return t
}

func (r *Replica) recover() (err error) {

	entries, err := r.redoLog.Read()
	if err != nil {
		return
	}

	txs := make(map[string]*Transaction)
	for idx, entry := range entries {
		log.WithFields(log.Fields{"idx":idx,"txId": entry.Id,"Op":entry.Op,"Key":entry.Key,"State":entry.State}).Info("[recover] entry:")
		txs[entry.Id] = r.parseTx(entry)
	}

	for txId, tx := range txs {

		logger := log.WithFields(log.Fields{"txId": tx.Id, "Op":tx.Op, "Key": tx.Key, "State": tx.State})
		logger.Info("[recover] transaction:")

		if tx.State == Prepared {
			txState := r.getStatus(txId)
			logger = logger.WithFields(log.Fields{"txState":txState})
			logger.Info("[recover] state == Prepared, call master to get status.")

			switch txState {
			case Aborted:
				tx.Op = RecoveryOp
				if err = r.abort(tx); err != nil {
					logger.WithError(err).Error("[recover] tx.Abort() failed.")
					continue
				}
				logger.Info("[recover] tx.Abort() success")
			case Committed:
				tx.Op = RecoveryOp
				if err = r.commit(tx); err != nil {
					logger.WithError(err).Error("[recover] tx.Commit() failed.")
					continue
				}
				logger.Info("[recover] tx.Commit() success")
			default:
				logger.Info("[recover] skip")
			}
		}
	}

	// 清理临时存储
	err = r.cleanUpTempStore()
	if err != nil {
		return
	}

	return
}

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {

	logger := log.WithFields(log.Fields{
		"key": args.Key,
	})

	val, ok := r.store.Get(args.Key)
	if !ok {
		logger.Error("[Get] key not exist")
	}

	reply.Value = val.(string)
	logger.WithField("value", val).Info("[Get] ok")
	return
}

func (r *Replica) Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	logger := log.WithFields(log.Fields{
		"key": args.Key,
	})
	reply.Value = args.Key
	logger.WithField("value", args.Key).Info("[Ping] ok")
	return nil
}

func (r *Replica) cleanUpTempStore() (err error) {
	keys := r.temp.Keys()
	for _, key := range keys {
		logger := log.WithFields(log.Fields{"key": key})
		txId, txKey := r.parseTempKey(key)
		tx, ok := r.txm[txId]
		if !ok || tx.State != Prepared {
			logger.WithFields(log.Fields{"txId": txId, "txKey":txKey, "ok":ok, "tx.State":tx.State}).Error("clean temp key.")
			r.temp.Del(key)
		}
	}
	return nil
}

// getStatus is only used during recovery to check the status from the Master
//
// 请求 master 查询事务状态
func (r *Replica) getStatus(txId string) TxState {

	client := NewMasterClient(MasterAddr)
	for retry := 0; retry < 3; {
		state, err := client.Status(txId)
		if err != nil {
			retry ++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return *state
	}

	return InValid
}
