package main

import (
	"fmt"
	"github.com/dchest/uniuri"
	"log"
	"os"
	"math/rand"
	"sync"
	"time"
)

type Master struct {
	Host string
	replicas []*ReplicaClient
	log      *logger
	txs      map[string]TxState
	didSuicide   bool
}

type Option struct {
	Port int
	LogPath  string // "logs/master.txt"
	Replicas []string
}

func NewMaster(opt *Option) *Master {

	host := fmt.Sprintf("%s:%d", "localhost", opt.Port)

	l := newLogger(opt.LogPath)

	clients := make([]*ReplicaClient, 0, len(opt.Replicas))
	for _, replica := range opt.Replicas {
		client, err :=  NewReplicaClient(replica)
		if err != nil {
			log.Println("[NewMaster] err=", err)
		}
		clients = append(clients, client)
	}

	return &Master{
		host,
		clients,
		l,
		make(map[string]TxState),
		false,
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


func (m *Master) recover() (err error) {

	// 读取事务日志
	entries, err := m.log.read()
	if err != nil {
		return
	}

	// 根据 entry 恢复 m.txs[]，因为日志是有序的，所以 m.txs[] 中保存了事务的最终状态
	for _, entry := range entries {
		log.Println("[recover] entry=", entry)
		tx := ParseTx(entry)
		m.txs[tx.Id] = tx.State
	}

	for txId, state := range m.txs {
		switch state {
		case Started:
			log.Printf("[recover] started tx, now call `m.sendAbort(txId)`, txId=%s, state=%s", txId, state)
			m.sendAbort("recover", txId)
		case Aborted:
			log.Printf("[recover] aborted tx, now call `m.sendAbort(txId)`, txId=%s, state=%s", txId, state)
			m.sendAbort("recover", txId)
		case Committed:
			log.Printf("[recover] commited tx, now call `m.sendAndWaitForCommit(txId)`, txId=%s, state=%s", txId, state)
			m.sendAndWaitForCommit("recover", txId, make([]ReplicaDeath, len(m.replicas)))
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
	log.Println("[Del] receive request: ", args.Key)
	var i int
	return m.DelTest(
		&DelTestArgs{
			args.Key,
			MasterDontDie,
			make([]ReplicaDeath, len(m.replicas)),
		}, &i)
}

func (m *Master) DelTest(args *DelTestArgs, _ *int) (err error) {
	log.Println("[DelTest] receive request: ", args.Key, args.MasterDeath, args.ReplicaDeaths)
	return m.mutate(
		DelOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryDel(args.Key, txId, rd)
		})
}

func (m *Master) Put(args *PutArgs, _ *int) (err error) {
	log.Printf("[Put] key=%s, value=%s",args.Key, args.Value)
	var i int
	return m.PutTest(
		&PutTestArgs{
			args.Key,
			args.Value,
			MasterDontDie,
			make([]ReplicaDeath, len(m.replicas)),
		}, &i)
}

func (m *Master) PutTest(args *PutTestArgs, _ *int) (err error) {

	log.Printf("[PutTest] key=%s, value=%s, MasterDeath=%v, ReplicaDeaths=%v",args.Key, args.Value, args.MasterDeath, args.ReplicaDeaths)

	return m.mutate(
		PutOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryPut(args.Key, args.Value, txId, rd)
		})
}

func getReplicaDeath(replicaDeaths []ReplicaDeath, n int) ReplicaDeath {
	rd := ReplicaDontDie
	if replicaDeaths != nil && len(replicaDeaths) > n {
		rd = replicaDeaths[n]
	}
	return rd
}

func (m *Master) mutate(

	operation Operation,
	key string,
	masterDeath MasterDeath,
	replicaDeaths []ReplicaDeath,
	f func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error),

) (err error) {



	action := operation.String()
	txId := uniuri.New()
	m.log.writeState(txId, Started)
	m.txs[txId] = Started


	// Send out all mutate requests in parallel.
	// If any abort, send on the channel.
	// Channel must be buffered to allow the non-blocking read in the switch.
	shouldAbort := make(chan int, len(m.replicas))
	log.Println("Master."+action+" asking replicas to "+action+" tx:", txId, "key:", key)


	// 并发调用，阻塞等待所有请求结束
	m.forEachReplica(

		func(i int, r *ReplicaClient) {
			// 调用 f()
			success, err := f(r, txId, i, getReplicaDeath(replicaDeaths, i))
			if err != nil {
				log.Println("Master."+action+" r.Try"+action+":", err)
			}
			// 如果失败，就需要回滚
			if success == nil || !*success {
				shouldAbort <- 1
			}
		},

	)


	// If at least one replica needed to abort
	select {
	// 失败，需要回滚
	case <-shouldAbort:
		log.Println("Master."+action+" asking replicas to abort tx:", txId, "key:", key)
		m.log.writeState(txId, Aborted)
		m.txs[txId] = Aborted
		m.sendAbort(action, txId)
		return TxAbortedError
	// 成功，需要提交（本地提交+远程提交）
	default:
		break
	}

	// The transaction is now officially committed
	m.dieIf(masterDeath, MasterDieBeforeLoggingCommitted) //???
	m.log.writeState(txId, Committed)
	m.dieIf(masterDeath, MasterDieAfterLoggingCommitted) //???
	m.txs[txId] = Committed

	// 发送 "commit" 给 replicas
	m.sendAndWaitForCommit(action, txId, replicaDeaths)
	return
}

// 发送 "abort" 给 replicas
func (m *Master) sendAbort(action string, txId string) {
	log.Println(fmt.Sprintf("[sendAbort] action=%s, txId=%s", action, txId))
	m.forEachReplica(func(i int, r *ReplicaClient) {
		_, err := r.Abort(txId)
		if err != nil {
			log.Println(fmt.Sprintf("[sendAbort] r.Abort(txId) failed, txId=%s, err=%v", txId, err))
		} else {
			log.Println(fmt.Sprintf("[sendAbort] r.Abort(txId) success, txId=%s, err=%v", txId, err))
		}
	})
}

// 发送 "commit" 给 replicas
func (m *Master) sendAndWaitForCommit(action string, txId string, replicaDeaths []ReplicaDeath) {
	log.Println(fmt.Sprintf("[sendAndWaitForCommit] action=%s, txId=%s, replicaDeaths=%v", action, txId, replicaDeaths))
	m.forEachReplica(func(i int, r *ReplicaClient) {
		for retry := 0; retry < 3; {
			_, err := r.Commit(txId, getReplicaDeath(replicaDeaths, i))
			if err == nil {
				log.Println(fmt.Sprintf("[sendAndWaitForCommit] r.Commit(txId) success, txId=%s, err=%v", txId, err))
				break
			}
			log.Println(fmt.Sprintf("[sendAndWaitForCommit] r.Commit(txId) failed, txId=%s, err=%v", txId, err))
			retry ++
			time.Sleep(100 * time.Millisecond)
		}
	})
}

func (m *Master) dieIf(actual MasterDeath, expected MasterDeath) {
	if !m.didSuicide && actual == expected {

		log.Println("Killing self as requested at", expected)

		m.log.writeSpecial(killedSelfMarker)

		os.Exit(1)
	}
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
	log.Println("[Ping] receive request: ", args.Key)
	reply.Value = args.Key
	return nil
}

func (m *Master) Status(args *StatusArgs, reply *StatusResult) (err error) {

	defer log.Printf("[Status] receive request, txId=%s, state=%s: ", args.TxId, reply.State)

	state, ok := m.txs[args.TxId]
	if !ok {
		log.Println("[Status] args.TxId is not found, so return \"INVALID\".")
		m.txs[args.TxId] = NoState
	}
	reply.State = state
	return nil
}
