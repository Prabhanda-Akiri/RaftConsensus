package raft

import (
	"labrpc"
	"time"
	"sync/atomic"
)

type RaftClient struct {
	id 		int
	peer  	*labrpc.ClientEnd
	msgChan chan AppendMessage
	voteChan chan RequestVoteArgs
	next 	int
	matched int
	lastAppendTime time.Time
	active	bool
	stop	int32
	pendingSnapshot int32
	raft  	*Raft
}

func (cl *RaftClient) PassAppendTimeout() bool {
	t := time.Now()
	// DPrintf("RaftClient PassAppendTimeout() where time: %d", t)
	// fmt.Printf("Client Timeout time = %f, client id = %d \n", t.Sub(cl.lastAppendTime).Seconds(),cl.id)
	// fmt.Printf(t.Sub(cl.lastAppendTime).Seconds())

	if t.Sub(cl.lastAppendTime).Seconds() > 1.0 {
		return true
	}
	return false
}

func (cl *RaftClient) Start() {
	// DPrintf("RaftClient Start()")
	//fmt.Printf("RaftClient Start id = %d \n",cl.id)
	cl.stop = 0
	cl.pendingSnapshot = 0
}

func (cl *RaftClient) Stop() {
	atomic.StoreInt32(&cl.stop, 1)
}

func (cl *RaftClient) sendSnapshot(msg AppendMessage) bool {
	var reply AppendReply
	ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	for !ok && atomic.LoadInt32(&cl.stop) == 0 &&
		atomic.LoadInt32(&cl.pendingSnapshot) == int32(msg.Snap.Index) {
		DPrintf("send AppendSnapshot failed from %d to %d, try again\n", msg.From, msg.To)
		ok = cl.peer.Call("Raft.AppendEntries", &msg, &reply)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
	if ok && atomic.LoadInt32(&cl.stop) == 0 {
		DebugPrint("send append msg success from %d to %d\n", msg.From, msg.To)
		cl.raft.msgChan <- reply
	}
	return ok
}

func (cl *RaftClient) sendAppendEntries(msg AppendMessage) bool {
	start := time.Now()
	var reply AppendReply
	DebugPrint("sendAppendEntries client id = %d\n", cl.id)
	ok := cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	ed := time.Now()
	if !ok && ed.Sub(start).Seconds() < 0.2 && atomic.LoadInt32(&cl.stop) == 0 {
		ok = cl.peer.Call("Raft.AppendEntries", &msg, &reply)
	}
	//calcRuntime(start, "sendAppendEntries")
	if ok && atomic.LoadInt32(&cl.stop) == 0 {
		//DebugPrint("send append msg success from %d to %d\n", msg.From, msg.To)
		cl.raft.msgChan <- reply
	}
	return ok
}

func (cl *RaftClient) sendRequestVote(args RequestVoteArgs) bool {
	//start := time.Now()
	var reply RequestVoteReply
	if args.MsgType == MsgRequestPrevote {
		reply.MsgType = MsgRequestPrevoteReply
	} else {
		reply.MsgType = MsgRequestVoteReply
	}
	//DebugPrint("begin send request vote from %d to %d \n", args.From, args.To)
	ok := cl.peer.Call("Raft.RequestVote", &args, &reply)
	//calcRuntime(start, "sendRequestVote")
	reply.To = args.To
	if ok && atomic.LoadInt32(&cl.stop) == 0 {
		cl.raft.voteChan <- reply
	} else {
		DebugPrint("send request vote from %d to %d, failed %v, %d\n", args.From, args.To, ok, atomic.LoadInt32(&cl.stop))
	}
	return ok
}

func (cl *RaftClient) AppendAsync(msg *AppendMessage) {
	if msg.MsgType == MsgAppend {
		cl.lastAppendTime = time.Now()
		go cl.sendAppendEntries(*msg)
	} else if msg.MsgType == MsgHeartbeat {
		go cl.sendAppendEntries(*msg)
	} else if msg.MsgType == MsgSnapshot {
		idx := int32(msg.Snap.Index)
		//if idx <= atomic.LoadInt32(&cl.pendingSnapshot) {
		////	DebugPrint("skip snapshot %d, because there is a bigger one: %d\n", idx, atomic.LoadInt32(&cl.pendingSnapshot))
		////	return
		//}
		atomic.StoreInt32(&cl.pendingSnapshot, idx)
		cl.lastAppendTime = time.Now()
		go cl.sendSnapshot(*msg)
	}
}

func (cl *RaftClient) VoteAsync(msg RequestVoteArgs) {
	go cl.sendRequestVote(msg)
}

