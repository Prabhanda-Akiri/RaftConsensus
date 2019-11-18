package raft

type MessageType int
const (
	_ MessageType = iota
	MsgStop
	MsgPropose
	MsgHeartbeat
	MsgHeartbeatReply
	MsgAppendReply
	MsgAppend
	MsgSnapshot
	MsgSnapshotReply
	MsgRequestVote
	MsgRequestVoteReply
	MsgRequestPrevote
	MsgRequestPrevoteReply
)

type AppendMessage struct {
	MsgType			MessageType
	Term			int
	From			int
	To 				int
	Commited		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Id          	int
	Entries			[]Entry
	Snap            Snapshot
}

type AppendReply struct {
	MsgType		MessageType
	Term        int
	From		int
	To 			int
	Commited	int
	Id          int
	Success		bool
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	MsgType			MessageType
	Term			int
	From			int
	To 				int
	LastLogIndex 	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	// Your data here (2A).
	MsgType		MessageType
	Term		int
	To 			int
	VoteGranted	bool
}

type DoneReply struct {}

type HardState struct {
	term 		int
	vote 		int
	commited	int
	size        int
}

type SnapshotMessage struct {
	MsgType			MessageType
	Term			int
	From			int
	To 				int
	Commited		int
}

func getMsgName(msgType MessageType) string {
	if msgType == MsgStop {
		return "Stop"
	} else if msgType == MsgRequestVote {
		return "RequestVote"
	} else if msgType == MsgRequestPrevote {
		return "RequestPreVote"
	} else if msgType == MsgRequestVoteReply {
		return "RequestVoteReply"
	} else if msgType == MsgRequestPrevoteReply {
		return "RequestPreVoteReply"
	} else if msgType == MsgAppend {
		return "AppendEntry"
	} else if msgType == MsgAppendReply {
		return "AppendReply"
	} else if msgType == MsgHeartbeat {
		return "Heartbeat"
	} else if msgType == MsgHeartbeatReply {
		return "HeartbeatReply"
	} else if msgType == MsgSnapshot {
		return "Snapshot"
	} else if msgType == MsgSnapshotReply {
		return "SnapshotReply"
	} else {
		return "Unkown"
	}
}

