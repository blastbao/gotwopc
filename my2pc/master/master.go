package master

import (
	"github.com/blastbao/gotwopc/my2pc/rpc"
	"log"
)

type Tx struct {
	tid    string
	key   string
	op    Operation
	state TxState
}

type KVStore struct {
	kvs map[string]string
}

type Replica struct {
	Id            	uint32

	Storage 		*KVStore
	tempStore       *KVStore

	txs            map[string]*Tx
	lockedKeys     map[string]bool
	log            *logger
	didSuicide     bool
}

type Master struct {



	replicas     []*rpc.ReplicaClient
	log          *logger
	txs          map[string]Tx
}



type Option struct {
	LogPath string // "logs/master.txt"
	Replicas [] string
}

func NewMaster(opt * Option ) *Master {
	l := newLogger(opt.LogPath)
	clients := make([]*rpc.ReplicaClient, len(opt.Replicas))
	for _, replica := range opt.Replicas {
		clients = append(clients, rpc.NewReplicaClient(replica))
	}

	return &Master{
		clients,
		l,
		make(map[string]Tx),
	}
}

func(m *Master) Run() {

	if len(m.replicas) <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	err := m.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	server.Register(master)

	log.Println("Master listening on port", MasterPort)
	http.ListenAndServe(MasterPort, server)
}




func (m *Master) recover() (err error) {

	// 从事务日志中，读取所有 entries
	entries, err := m.log.read()
	if err != nil {
		return
	}

	//
	m.didSuicide = false

	// 遍历 entries ，如果某个 entry 是
	for _, entry := range entries {

		// 如果 entry
		switch entry.txId {
		case killedSelfMarker:
			m.didSuicide = true
			continue
		case firstRestartAfterSuicideMarker:
			m.didSuicide = false
			continue
		}

		// 根据 entry 恢复 m.txs[]，因为日志是有序的，所以 m.txs[] 中保存了事务的最终状态
		m.txs[entry.txId] = entry.state
	}

	//
	for txId, state := range m.txs {
		switch state {
		case Started:
			fallthrough
		case Aborted:
			log.Println("Aborting tx", txId, "during recovery.")
			m.sendAbort("recover", txId)
		case Committed:
			log.Println("Committing tx", txId, "during recovery.")
			m.sendAndWaitForCommit("recover", txId, make([]ReplicaDeath, m.replicaCount))
		}
	}

	//
	if m.didSuicide {
		m.log.writeSpecial(firstRestartAfterSuicideMarker)
	}

	return
}