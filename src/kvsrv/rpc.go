package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	ClientMeta ClientMeta
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

type ClientMeta struct {
	EndName string
	OpIndex uint
}
