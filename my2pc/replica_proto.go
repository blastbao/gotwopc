package main

type IReplicaClient interface {
	Ping(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error)
	TryPut(args *TxPutArgs, reply *ReplicaActionResult) (err error)
	TryDel(args *TxDelArgs, reply *ReplicaActionResult) (err error)
	Commit(args *CommitArgs, reply *ReplicaActionResult) (err error)
	Abort(args *AbortArgs, reply *ReplicaActionResult) (err error)
	Get(args *ReplicaKeyArgs, reply *ReplicaGetResult) (err error)
}

type TxPutArgs struct {
	Key   string
	Value string
	TxId  string
}

type TxDelArgs struct {
	Key  string
	TxId string
}

type CommitArgs struct {
	TxId string
}

type AbortArgs struct {
	TxId string
}

type ReplicaKeyArgs struct {
	Key string
}

type ReplicaGetResult struct {
	Value string
}

type ReplicaActionResult struct {
	Success bool
}



