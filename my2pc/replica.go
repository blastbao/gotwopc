package main

import (
	"errors"
	"fmt"
	"sync"

	//"log"
	"os"
	"strings"
	"time"
	log "github.com/sirupsen/logrus"
)

func init(){
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
}

type Replica struct {
	Host string
	num            int
	committedStore *keyValueStore
	tempStore      *keyValueStore
	txs            map[string]*Tx
	lockedKeys     map[string]bool
	log            *logger
	didSuicide     bool

	locks 			 sync.Map
	txm              map[string]*Tx
	store            *MemStore
	temp             *MemStore
}

func (r *Replica) run() {
	err := r.recover()
	if err != nil {
		log.WithError(err).Fatal("[run] r.recover() failed.")
	}
	log.WithError(err).WithFields(log.Fields{"replicaNum":r.num, "host":r.Host}).Info("[run] ok.")
	Run(r, r.Host)
}

func NewReplica(num int) *Replica {
	host := GetReplicaHost(num)
	l := newLogger(fmt.Sprintf("logs/replica%v.txt", num))
	return &Replica{
		host,
		num,
		newKeyValueStore(fmt.Sprintf("data/replica%v/committed", num)),
		newKeyValueStore(fmt.Sprintf("data/replica%v/temp", num)),
		make(map[string]*Tx),
		make(map[string]bool),
		l,
		false,
		sync.Map{},
		make(map[string]*Tx),
		NewMemStore(),
		NewMemStore(),
	}
}

func (r *Replica) getTempStoreKey(txId string, key string) string {
	return txId + "__" + key
}

func (r *Replica) parseTempStoreKey(key string) (txId string, txKey string) {
	split := strings.Split(key, "__")
	return split[0], split[1]
}


func (r *Replica) TTryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {

	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"Key":args.Key,
		"Value": args.Value,
		"die":args.Die,
	})

	writeTemp := func() error {
		r.temp.Put(r.getTempStoreKey(args.TxId, args.Key), args.Value)
		return nil
	}

	err = r.tryMutate(args.Key, args.TxId, args.Die, PutOp, writeTemp, reply)
	logger.WithField("rsp",  reply.Success).Info("[TryPut] ok")
	return
}



func (r *Replica) TAbort(txId string, keys ... string) {
	r.txm[txId].State = Aborted
	r.log.writeState(txId, Aborted)
	for _, key := range keys{
		r.locks.Delete(key)
	}
}

func (r *Replica) TTry(txId string, key string, op Operation, f func() error) (rsp*ReplicaActionResult, err error) {

	logger := log.WithFields(log.Fields{
		"txId":txId,
		"Key":key,
		"op":op,
	})

	rsp = &ReplicaActionResult{
		Success: false,
	}

	// 构造事务对象
	r.txm[txId] = &Tx{
		Id: txId,
		Key: key,
		Op: op,
		State: Started,
	}

	// 资源锁定
	if _, locked := r.locks.LoadOrStore(key, struct{}{}); locked {
		logger.Error("[TTry] key is locked by others, Abort.")
		r.TAbort(txId)
		return
	}

	// 执行事务逻辑
	if f != nil {
		err = f()
		if err != nil {
			logger.WithError(err).Error("[TTry] call f() failed, so abort and release resource.")
			r.TAbort(txId, key)
			return
		}
	}

	// 将状态由 "Started" 更新为 "Prepared"
	r.txs[txId].State = Prepared
	r.log.writeOp(txId, Prepared, op, key)

	// 回复
	rsp.Success = true
	logger.WithField("rsp", rsp.Success).Info("[TTry] ok.")

	return
}






func (r *Replica) TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error) {

	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"Key":args.Key,
		"Value": args.Value,
		"die":args.Die,
	})

	// 写临时存储
	writeToTempStore := func() error {
		return r.tempStore.put(r.getTempStoreKey(args.TxId, args.Key), args.Value)
	}
	err = r.tryMutate(args.Key, args.TxId, args.Die, PutOp, writeToTempStore, reply)
	logger.WithField("rsp",  reply.Success).Info("[TryPut] ok")
	return
}

func (r *Replica) TryDel(args *TxDelArgs, reply *ReplicaActionResult) (err error) {

	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"Key":args.Key,
		"die":args.Die,
	})

	err = r.tryMutate(args.Key, args.TxId, args.Die, DelOp, nil, reply)
	logger.WithField("rsp",  reply.Success).Info("[TryPut] ok")
	return
}

func (r *Replica) tryMutate(key string, txId string, die ReplicaDeath, op Operation, f func() error, reply *ReplicaActionResult) (err error) {


	logger := log.WithFields(log.Fields{
		"txId":txId,
		"Key":key,
		"die":die,
		"op":op,
	})

	r.dieIf(die, ReplicaDieBeforeProcessingMutateRequest)

	reply.Success = false

	// 存储事务信息
	r.txs[txId] = &Tx{
		txId,
		key,
		op,
		Started, // "开始"
	}

	// 查询资源，若已被其它事务锁定，则 abort 当前事务
	if _, ok := r.lockedKeys[key]; ok {
		// Key is currently being modified, Abort
		logger.Error("[tryMutate] key is locked by other tx, Abort current tx.")
		r.txs[txId].State = Aborted
		r.log.writeState(txId, Aborted)
		return nil
	}

	// 锁定资源（？这块非原子操作...）
	r.lockedKeys[key] = true

	// 执行 f() 处理资源，若失败，则 abort ，并释放资源
	if f != nil {
		err = f()
		if err != nil {
			logger.WithError(err).Error("[tryMutate] call f() failed, so abort and release resource.")
			r.txs[txId].State = Aborted
			r.log.writeState(txId, Aborted)
			delete(r.lockedKeys, key)
			return
		}
	}

	// 执行 f() 成功，将事务状态由 "Started" 更新为 "Prepared"，回复成功给 master
	r.txs[txId].State = Prepared
	r.log.writeOp(txId, Prepared, op, key)
	reply.Success = true

	r.dieIf(die, ReplicaDieAfterLoggingPrepared)

	logger.WithField("rsp", reply.Success).Info("[tryMutate] ok.")

	return
}

func (r *Replica) Commit(args *CommitArgs, reply *ReplicaActionResult) (err error) {
	logger := log.WithFields(log.Fields{
		"txId":args.TxId,
		"die":args.Die,
	})


	r.dieIf(args.Die, ReplicaDieBeforeProcessingCommit)

	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Error! We've never heard of this transaction
		logger.Error("[Commit] txId not exist.")
		return errors.New(fmt.Sprint("Received commit for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.Key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received commit for transaction with unlocked key:", txId)
	}

	switch tx.State {
	case Prepared:
		err = r.commit(txId, tx.Op, tx.Key, args.Die)
	default:
		log.Println("Received commit for transaction in state ", tx.State.String())
	}

	if err == nil {
		reply.Success = true
	}

	return
}

func (r *Replica) commit(txId string, op Operation, key string, die ReplicaDeath) (err error) {

	logger := log.WithFields(log.Fields{"txId":txId, "Key":key, "op": op, "die":die})

	// 释放资源锁定
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		// 将 key 从临时存储移入到持久存储中
		val, err := r.tempStore.get(r.getTempStoreKey(txId, key))
		if err != nil {
			logger.WithError(err).Error("[commitTx] r.tempStore.get() failed.")
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", key))
		}
		err = r.committedStore.put(key, val)
		if err != nil {
			logger.WithError(err).Error("[commitTx] r.committedStore.put() failed.")
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", key))
		}
		logger.WithField("val", val).Info("[commitTx] PutOp() ok.")

	case DelOp:
		// 将 key 从持久存储中移除
		err = r.committedStore.del(key)
		if err != nil {
			logger.WithError(err).Error("[commitTx] r.committedStore.del() failed.")
			return errors.New(fmt.Sprint("Unable to commit del val for tx:", txId, "key:", key))
		}
		logger.Info("[commitTx] DelOp() ok.")
	}

	// 更新事务状态
	r.log.writeState(txId, Committed)

	// 移除缓存的事务对象
	delete(r.txs, txId)

	// Delete the temp data only after committed, in case we crash after deleting, but before committing
	//
	// 只有在提交后才会删除临时数据，以防止我们在删除后、提交前崩溃。
	if op == PutOp {
		err = r.tempStore.del(r.getTempStoreKey(txId, key))
		r.dieIf(die, ReplicaDieAfterDeletingFromTempStore)
		if err != nil {
			logger.WithError(err).Error("r.tempStore.del() failed.")
		}
	}

	r.dieIf(die, ReplicaDieAfterLoggingCommitted)
	return nil
}

func (r *Replica) Abort(args *AbortArgs, reply *ReplicaActionResult) (err error) {
	defer log.Printf("[Commit] receive request, TxId=%s, Rsp=%v: ", args.TxId, reply.Success)


	reply.Success = false

	txId := args.TxId

	// 取出事务对象
	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Shouldn't happen, we've never heard of this transaction
		return errors.New(fmt.Sprint("Received abort for unknown transaction:", txId))
	}

	// 检查资源锁定情况
	_, keyLocked := r.lockedKeys[tx.Key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("[Abort] Received abort for transaction with unlocked key:", txId)
	}

	// 检查事务状态，如果是 "Prepared" 状态，则尚未提交，直接 abort，其它状态不予处理
	switch tx.State {
	case Prepared:
		r.abort(txId, tx.Op, tx.Key)
	default:
		log.Println("[Abort] Received abort for transaction in state ", tx.State.String())
	}

	reply.Success = true
	return nil
}

func (r *Replica) abort(txId string, op Operation, key string) {

	// 释放资源
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		// We no longer need the temp stored value
		// 删除临时存储
		err := r.tempStore.del(r.getTempStoreKey(txId, key))
		if err != nil {
			fmt.Println("Unable to del val for uncommitted tx:", txId, "key:", key)
		}
	//case DelOp:
		// nothing to undo here
	}

	// 更新事务状态到日志
	r.log.writeState(txId, Aborted)

	// 移除事务对象
	delete(r.txs, txId)
}

func (r *Replica) Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	defer log.Printf("[Get] receive request, Key=%s, Value=%v", args.Key, reply.Value)
	val, err := r.committedStore.get(args.Key)
	if err != nil {
		log.Printf("[Get] r.committedStore.get() fail, Key=%s, err=%v: ", args.Key, err)
		return
	}
	reply.Value = val
	return
}

func (r *Replica) Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error) {
	defer log.Printf("[Ping] receive request, Key=%s, Value=%v: ", args.Key, reply.Value)

	reply.Value = args.Key
	return nil
}


func (r *Replica) recover() (err error) {

	entries, err := r.log.read()
	if err != nil {
		log.WithError(err).Error("[recover] r.log.read() failed.")
		return
	}

	txs := make(map[string]*Tx)
	for idx, entry := range entries {
		log.WithFields(log.Fields{"idx":idx,"txId": entry.Id,"Op":entry.Op,"Key":entry.Key,"State":entry.State}).Info("entry:")
		tx := ParseTx(entry)
		txs[tx.Id] = tx
	}

	for txId, tx := range txs {

		logger := log.WithFields(log.Fields{"txId": tx.Id, "Op":tx.Op, "Key": tx.Key, "State": tx.State})
		logger.Info("tx:")

		if tx.State == Prepared {

			logger.Info("[recover] state == Prepared, call master to get status.")

			txState := r.getStatus(txId)
			switch txState {
			case Aborted:
				logger.Info("[recover] tx is aborted, call r.abortTx().")
				r.abort(tx.Id, RecoveryOp, tx.Key)
			case Committed:
				logger.Info("[recover] tx is committed, call r.commitTx().")
				err := r.commit(tx.Id, RecoveryOp, tx.Key, ReplicaDontDie)
				if err != nil {
					logger.WithError(err).Error("[recover] r.commitTx() failed.")
				} else {
					logger.Info("[recover] r.commitTx() success")
				}
			default:
				// 其它状态，则不予处理
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


func (r *Replica) recoverv2() (err error) {

	// 解析日志
	entries, err := r.log.read()
	if err != nil {
		return
	}

	// ???
	r.didSuicide = false

	for _, entry := range entries {

		logger := log.WithFields(log.Fields{"txId":entry.Id, "Key":entry.Key, "State":entry.State})

		switch entry.Id {
		case killedSelfMarker:
			r.didSuicide = true
			continue
		case firstRestartAfterSuicideMarker:
			r.didSuicide = false
			continue
		}

		// 若某条记录为 "Prepared" 状态，需要请求 master 查询事务状态，查询失败会返回 "INVALID" 状态。
		if entry.State == Prepared {
			log.Info("[recover] state == Prepared, call master to get status.")
			entry.State = r.getStatus(entry.Id)
			switch entry.State {
			case Aborted:
				logger.Info("[recover] tx is aborted, call r.abortTx().")
				r.abort(entry.Id, RecoveryOp, entry.Key)
			case Committed:
				logger.Info("[recover] tx is committed, call r.commitTx().")
				err := r.commit(entry.Id, RecoveryOp, entry.Key, ReplicaDontDie)
				if err != nil {
					logger.WithError(err).Error("[recover] r.commitTx() failed.")
				} else {
					logger.Info("[recover] r.commitTx() success")
				}
			default:
				// 其它状态，则不予处理
			}
		}

		switch entry.State {
		case Started:
		case Prepared:
		case Committed:
			r.txs[entry.Id] = &Tx{entry.Id, entry.Key, entry.Op, Committed}
		case Aborted:
			r.txs[entry.Id] = &Tx{entry.Id, entry.Key, entry.Op, Aborted}
		}
	}

	// 清理临时存储
	err = r.cleanUpTempStore()
	if err != nil {
		return
	}

	if r.didSuicide {
		r.log.writeSpecial(firstRestartAfterSuicideMarker)
	}

	return
}

func (r *Replica) cleanUpTempStore() (err error) {

	// 获取临时存储中的所有 keys
	keys, err := r.tempStore.list()
	if err != nil {
		return
	}

	// 根据 key 算出其关联的事务 txId，
	// 若该事务不存在或者非 "prepared" 状态，则从临时存储中删除当前 key 。
	for _, key := range keys {
		txId, _ := r.parseTempStoreKey(key)
		tx, ok := r.txs[txId]
		if !ok || tx.State != Prepared {
			//println("[cleanUpTempStore] Cleaning up temp key ", key)
			err = r.tempStore.del(key)
			if err != nil {
				return
			}
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

	return NoState
}



func (r *Replica) dieIf(actual ReplicaDeath, expected ReplicaDeath) {

	if !r.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		r.log.writeSpecial(killedSelfMarker)
		os.Exit(1)
	}

}
