package main

import (
	"errors"
	"net/http"
	"net/rpc"


	log "github.com/sirupsen/logrus"
)

type Transaction struct {
	Id          string
	Key   		string
	Op    		Operation
	State 		TxState
	F func() error

	Redo     *RedoLog
	Lock     *LockMgr
	logger   *log.Entry
}

func (t *Transaction) Start() error {
	if t.Lock.Lock(t.Key) {
		t.logger.Error("t.LockMgr.Lock(t.Key) failed.")
		t.Abort()
		return errors.New("key is locked by others")
	}
	t.State = Started
	// 执行事务逻辑
	if t.F != nil {
		if err := t.F(); err != nil {
			t.logger.WithError(err).Error("t.F() failed.")
			t.Abort()
			t.Lock.Unlock(t.Key)
			return err
		}
		t.logger.Info("[Transaction] t.F() ok.")
	}
	t.Redo.Write(t)
	return nil
}

func (t *Transaction) Prepare() error {
	t.State = Prepared
	t.Redo.Write(t)
	return nil
}

func (t *Transaction) Abort() error {
	t.State = Aborted
	t.Redo.Write(t)
	return nil
}

func (t *Transaction) Commit() error {
	t.State = Committed
	t.Redo.Write(t)
	t.Lock.Unlock(t.Key)
	return nil
}





type Tx struct {
	Id    string
	Key   string
	Op    Operation
	State TxState
}




type TxState int

const (
	InValid TxState = iota
	Started
	Prepared
	Committed
	Aborted
)

func (s TxState) String() string {
	switch s {
	case Started:
		return "STARTED"
	case Prepared:
		return "PREPARED"
	case Committed:
		return "COMMITTED"
	case Aborted:
		return "ABORTED"
	}
	return "INVALID"
}

func ParseTxState(s string) TxState {
	switch s {
	case "STARTED":
		return Started
	case "PREPARED":
		return Prepared
	case "COMMITTED":
		return Committed
	case "ABORTED":
		return Aborted
	}
	return InValid
}

type Operation int

const (
	NoOp Operation = iota
	PutOp
	DelOp
	RecoveryOp
)

func (s Operation) String() string {
	switch s {
	case PutOp:
		return "PUT"
	case DelOp:
		return "DEL"
	case RecoveryOp:
		return "RECOVERY"
	}
	return "INVALID"
}

func ParseOperation(s string) Operation {
	switch s {
	case "PUT":
		return PutOp
	case "DEL":
		return DelOp
	case "RECOVERY":
		return RecoveryOp
	}
	return NoOp
}

var (
	TxAbortedError = errors.New("Transaction aborted.")
)

func Run(svr interface{}, addr string) {

	server := rpc.NewServer()
	server.Register(svr)

	//log.Println("[RunSvr] run svr on:", addr)
	err := http.ListenAndServe(addr, server)
	if err != nil {
		log.Fatal(err)
	}
}

type IReplicaMaster interface {
	Ping(args *PingArgs, reply *GetResult) (err error)
	Status(args *StatusArgs, reply *StatusResult) (err error)
	Put(args *PutArgs, _ *int) (err error)
	PutTest(args *PutTestArgs, _ *int) (err error)
	Get(args *GetArgs, reply *GetResult) (err error)
	GetTest(args *GetTestArgs, reply *GetResult) (err error)
	Del(args *DelArgs, _ *int) (err error)
	DelTest(args *DelTestArgs, _ *int) (err error)
}
type ReplicaDeath int

const (

	ReplicaDontDie ReplicaDeath = iota

	// During mutation
	ReplicaDieBeforeProcessingMutateRequest
	ReplicaDieAfterLoggingPrepared

	// During commit
	ReplicaDieBeforeProcessingCommit
	ReplicaDieAfterDeletingFromTempStore
	ReplicaDieAfterLoggingCommitted
)

type MasterDeath int

const (
	MasterDontDie MasterDeath = iota
	MasterDieBeforeLoggingCommitted
	MasterDieAfterLoggingCommitted
)

var killedSelfMarker = "::justkilledself::"
var firstRestartAfterSuicideMarker = "::firstrestartaftersuicide::"
