package main

type PutArgs struct {
	Key   string
	Value string
}

type PutTestArgs struct {
	Key           string
	Value         string
}

type GetArgs struct {
	Key string
}

type GetTestArgs struct {
	Key        string
	ReplicaNum int
}

type DelArgs struct {
	Key string
}

type DelTestArgs struct {
	Key           string
}

type StatusArgs struct {
	TxId string
}

type StatusResult struct {
	State TxState
}

type PingArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

