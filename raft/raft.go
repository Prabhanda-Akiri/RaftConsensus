package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"bytes"
	"labgob"
	"fmt"
	"crypto/sha256"
	"encoding/hex"
)
// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	LogIndex     int
	Term         int
	Snap *Snapshot
}

var debugMode = false

func DebugPrint(format string, a ...interface{}) {
	if debugMode {
		fmt.Printf(format, a...)
	}
}

//
// A Go object implementing a single Raft peer.
//

//
// A Go object implementing a single Raft peer.
//

type RoleState int
const (
	_ RoleState = iota
	Leader
	Candidate
	PreCandidate
	Follower
)


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	clients		[]RaftClient

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term	  int
	vote 	  int
	leader    int
	state	  RoleState
	prevState HardState
	electionTimeout int32
	rdElectionTimeout int32
	lastHeartBeat time.Time
	lastElection time.Time
	applySM    chan ApplyMsg
	msgChan    chan AppendReply
	voteChan    chan RequestVoteReply
	stopChan    chan bool
	raftLog	  UnstableLog
	votes	  []int
	stop 		bool
	failCount   int32
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) reset(term int)  {
	rf.term = term
	for idx := range rf.votes {
		rf.votes[idx] = -1
	}
	rf.lastHeartBeat = time.Now()
	rf.lastElection = time.Now()
	rf.vote = -1
}

func (rf *Raft) IsLeader() bool {
	return rf.leader == rf.me && rf.state == Leader
}

func (rf *Raft) IsCandidate() bool {
	return rf.state == Candidate || rf.state == PreCandidate
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPrint("%d Get term: %d,  state: %d\n", rf.me, rf.term, rf.state)
    return rf.term, rf.state == Leader
}

func (rf *Raft) GetLeader() int {
	return rf.leader;
}

func (rf *Raft) DebugLog() {
	DebugPrint("=======%d, log size: %d, commit: %d, applied: %d\n",
		rf.me, rf.raftLog.Size(), rf.raftLog.commited, rf.raftLog.applied)
	DebugPrint("=======%d,  state: %d, leader: %d, term: %d\n",
		rf.me, rf.state, rf.leader, rf.term)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftStateData())
	DebugPrint("%d save to %d, %d, %d, %d\n", rf.me, rf.term, rf.vote, rf.raftLog.commited, rf.raftLog.size)
}


func (rf *Raft) getRaftStateData() []byte {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.term)
	e.Encode(rf.vote)
	e.Encode(rf.raftLog.commited)
	e.Encode(rf.raftLog.size)
	e.Encode(rf.raftLog.GetUnstableEntries())
	return w.Bytes()
}
//
// restore previously persisted state.
//
func (rf *Raft) recoverFromPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.term)
	d.Decode(&rf.vote)
	d.Decode(&rf.raftLog.commited)
	d.Decode(&rf.raftLog.size)
	d.Decode(&rf.raftLog.Entries)
	rf.prevState = HardState{rf.term, rf.vote, rf.raftLog.commited, rf.raftLog.Size()}
	rf.raftLog.applied = 0
	DebugPrint("%d recover from %d, %d, %d, %d\n",
		rf.me, rf.term, rf.vote, rf.raftLog.commited, rf.raftLog.size)
}

func (rf *Raft) recoverFromSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.mu.Lock()
		rf.stop = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	s := MakeSnapshot(data)
	rf.raftLog.snapshot = s
	var msg ApplyMsg
	msg.CommandValid = false
	msg.Snap = s

	rf.applySM <- msg
	rf.raftLog.applied = s.Index
	entries := rf.raftLog.GetUnApplyEntry()
	for _, e := range entries {
		m := rf.createApplyMsg(e)
		if m.CommandValid {
			rf.applySM <- m
		}
		rf.raftLog.applied += 1
	}
	rf.stop = false
	rf.mu.Unlock()
	DebugPrint("%d recover snapshot(%d) applied: %d, commit: %d, %d\n",
		rf.me, len(s.Data), rf.raftLog.applied, rf.raftLog.commited, rf.raftLog.size)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//


//
// example RequestVote RPC handler.
//
func calcRuntime(t time.Time, f string) {
	//now := time.Now()
	//log.Printf("%s cost %f millisecond\n", f, now.Sub(t).Seconds() * 1000)
	//log.Printf()
}

func CalcRuntime(t time.Time, f string) {
	calcRuntime(t, f)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	start := time.Now()
	defer calcRuntime(start, "RequestVote")
	DebugPrint("%d(%d) AccessRequest(%s) vote from %d(%d)\n", rf.me, rf.term, getMsgName(args.MsgType), args.From, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.maybeChange()
	reply.To = rf.me
	if !rf.checkVote(args.From, args.Term, args.MsgType, &reply.VoteGranted) || rf.state == Leader {
		reply.Term = rf.term
		DebugPrint("%d %d reject smaller term: %d\n", rf.me, rf.term, args.Term)
		return
	}
	if ((rf.leader == -1 && rf.vote == -1) || rf.vote == args.From ||
		(args.MsgType == MsgRequestPrevote && rf.term < args.Term)) &&
		rf.raftLog.IsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		DebugPrint("%d (leader:%d, vote: %d, state: %d) agree vote for: %d\n", rf.me, rf.leader,
			rf.vote, rf.state, args.From)
		reply.VoteGranted = true
		reply.Term = args.Term
		if args.MsgType == MsgRequestVote {
			rf.vote = args.From
			rf.lastElection = time.Now()
		}
		return
	}
	DebugPrint("%d reject vote for: %d, leader: %d, vote: %d\n", rf.me, args.From, rf.leader, rf.vote)
	reply.VoteGranted = false
	reply.Term = rf.term
}

func getResponseType(msg MessageType) MessageType {
	if msg == MsgAppend {
		return MsgAppendReply
	} else if msg == MsgHeartbeat {
		return MsgHeartbeatReply
	} else if msg == MsgSnapshot {
		return MsgAppendReply
	} else if msg == MsgRequestVote {
		return MsgRequestVoteReply
	} else if msg == MsgRequestPrevote {
		return MsgRequestPrevoteReply
	}
	return MsgStop
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


func (rf *Raft) handleVoteReply(reply* RequestVoteReply) {
	DebugPrint("%d(%d): receive vote reply from %d(%d), state: %d\n",
		rf.me, rf.term, reply.To, reply.Term, rf.state)
	start := time.Now()
	defer calcRuntime(start, "handleVoteReply")
	if !rf.checkVote(reply.To, reply.Term, reply.MsgType, &reply.VoteGranted) {
		return
	}
	if (rf.state == Candidate && reply.MsgType == MsgRequestVoteReply) ||
		(rf.state == PreCandidate && reply.MsgType == MsgRequestPrevoteReply) {
		DebugPrint("%d(%d): access vote reply from %d(%d), accept: %t, state: %d\n",
			rf.me, rf.term, reply.To, reply.Term, reply.VoteGranted, rf.state)
		if reply.VoteGranted {
			rf.votes[reply.To] = 1
		} else {
			rf.votes[reply.To] = 0
		}
		quorum := len(rf.peers) / 2 + 1
		accept := 0
		reject := 0
		for _, v := range rf.votes {
			if v == 1 {
				accept += 1
			} else if v == 0 {
				reject += 1
			}
		}
		if accept >= quorum {
			for idx, v := range rf.votes {
				if v == 1 {
					DebugPrint("%d vote for me(%d).\n", idx, rf.me)
				}
			}
			DebugPrint("%d win.\n", rf.me)
			if rf.state == PreCandidate {
				fmt.Printf("The server %d, wins Pre-vote Election\n", rf.me)
				rf.campaign(MsgRequestVote)
			} else {
				DebugPrint("%d win vote\n", rf.me)
				rf.becomeLeader()
				fmt.Printf("The server %d, wins Election\n", rf.me)
				// rf.propose(nil, rf.raftLog.GetDataIndex())
				rf.proposeNew(nil, rf.raftLog.GetDataIndex(), rf.me)
			}
		} else if reject == quorum {
			DebugPrint("%d has been reject by %d members\n", rf.me, reject)
			rf.becomeFollower(rf.term, -1)
		}
	}
	DebugPrint("%d(%d): receive vote end\n", rf.me, rf.term)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if !rf.IsLeader() {
		return rf.raftLog.Size(), rf.term, false
	}
	rf.mu.Lock()
	index := rf.raftLog.GetDataIndex() + 1
	DebugPrint("%d Store a message, at index: %d, term: %d\n",
		rf.me, index, rf.term)
	//rf.propose(command, index)
	rf.proposeNew(command, index,rf.me)	
	rf.mu.Unlock()
	return index, rf.term, true
}

func (rf *Raft) CreateSnapshot(data []byte, index int, maxRaftState int)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stop {
		return
	}
	if rf.raftLog.snapshot != nil && rf.raftLog.snapshot.Index >= index {
		return
	}
	if rf.stop || rf.persister.RaftStateSize() < maxRaftState {
		return
	}
	DebugPrint("%d create snapshot which index to %d, snapshot size: %d, log size: %d, last index %d, applied: %d, commit: %d\n",
		rf.me, index, len(data),rf.raftLog.Size(), rf.raftLog.GetLastIndex(), rf.raftLog.applied, rf.raftLog.commited)

	start := time.Now()
	term := rf.raftLog.GetEntry(index).Term
	s := &Snapshot{index, rf.raftLog.GetEntry(index).DataIndex, term, data, rf.raftLog.GetEntry(index).Timestamp, rf.raftLog.GetEntry(index).BPM, rf.raftLog.GetEntry(index).Hash, rf.raftLog.GetEntry(index).PrevHash,rf.me}
	rf.raftLog.SetSnapshot(s)
	rf.persister.SaveStateAndSnapshot(rf.getRaftStateData(), s.Bytes())
	calcRuntime(start, "CreateSnapshot")
	return
}

func (rf *Raft) createApplyMsg(e Entry) ApplyMsg {
	var applyMsg ApplyMsg
	if e.Data != nil {
		applyMsg.CommandIndex = e.DataIndex
		applyMsg.LogIndex = e.Index
		applyMsg.Command = e.Data
		applyMsg.Term = e.Term
		applyMsg.CommandValid = true
		//DebugPrint("%d Apply entre : term: %d, index: %d, value : %d\n", rf.me, e.Term, applyMsg.CommandIndex, tmp)
	} else {
		applyMsg.Command = -1
		applyMsg.CommandValid = false
		//applyMsg.CommandValid = false
		DebugPrint("%d empty Apply entre : term: %d, index: %d, value\n", rf.me, e.Term, e.Index)
	}
	return applyMsg
}

func MaxInt(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}


func (rf *Raft) appendMore(idx int) {
	snap := rf.raftLog.GetSnapshot()
	if snap != nil && rf.clients[idx].next <= snap.Index {
		msg := rf.createMessage(idx, MsgSnapshot)
		msg.Snap = *snap
		msg.Commited = rf.raftLog.commited
		DebugPrint("%d send AppendSnapshot to %d since %d, which matched (%d, %d)\n",
			rf.me, idx, rf.clients[idx].next, rf.clients[idx].matched, rf.raftLog.commited)
		rf.clients[idx].AppendAsync(&msg)
	} else if rf.clients[idx].next <= rf.raftLog.GetLastIndex() {
		msg := rf.createMessage(idx, MsgAppend)
		msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(rf.clients[idx].next)
		DebugPrint("%d send AppendEntries to %d since %d, which matched (%d, %d)\n",
			rf.me, idx, rf.clients[idx].next, rf.clients[idx].matched, rf.raftLog.commited)
		msg.PrevLogTerm = rf.raftLog.GetEntry(msg.PrevLogIndex).Term
		msg.Commited = rf.raftLog.commited
		rf.clients[msg.To].AppendAsync(&msg)
	}
}

func (rf *Raft) checkVote(from int, term int, msgType MessageType, accept* bool) bool {
	if term > rf.term {
		t := time.Now()
		if msgType == MsgRequestVote || msgType == MsgRequestPrevote{
			if !rf.passed_election_time(rf.electionTimeout, t) && rf.leader != -1 {
				*accept = false
				return false
			}
			DebugPrint("%d(%d, leader: %d) access a msg (%s) from %d, term:%d. when %v, since last heartbeat: %v\n",
				rf.me, rf.leader, rf.term, getMsgName(msgType), from, term, t, rf.lastElection)
		}
		//DebugPrint("%d(%d) receive a larger term(%d) from %d of %s, current leader: %d\n",
		//	rf.me, rf.term, term, from, getMsgName(msgType), rf.leader)
		if msgType == MsgRequestPrevote || (msgType == MsgRequestPrevoteReply && *accept == true) {

		} else {
			rf.becomeFollower(term, -1)
		}
	} else if term < rf.term && msgType == MsgRequestPrevote {
		//if msgType == MsgAppend || msgType == MsgHeartbeat
		*accept = false
		return false
	}
	return true;
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	for idx := range rf.clients {
		if idx != rf.me {
			rf.clients[idx].Stop()
		}
	}
	DebugPrint("Kill Raft %d, fail rpc: %d\n", rf.me, rf.failCount)
	//for ts := 1; atomic.LoadInt32(&rf.stop) != 2 && ts < 20; ts ++ {
	//	time.Sleep(1000 * time.Millisecond)
	//}
	rf.mu.Lock()
	rf.stop = true
	rf.mu.Unlock()
	rf.stopChan <- true
	DebugPrint("Kill Raft %d\n", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) becomeFollower(term int, leader int) {
	rf.reset(term)
	rf.state = Follower
	rf.leader = leader
	DebugPrint("%d become follower of %d in term: %d\n", rf.me, leader, term)
}

func (rf *Raft) becomeLeader() {
	index := rf.raftLog.GetLastIndex()
	for idx := range rf.clients {
		pr := &rf.clients[idx]
		pr.next = index + 1
		pr.active = false
		if idx == rf.me {
			pr.matched = index
		} else{
			pr.matched = 0
		}
	}
	DebugPrint("%d become leader at %d\n", rf.me, rf.term)
	//time.Sleep(10 * time.Millisecond)
	rf.state = Leader
	rf.leader = rf.me
	rf.lastHeartBeat = time.Now()
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeCandidate(msgType MessageType) int {
	term := rf.term + 1
	if msgType == MsgRequestPrevote {
		rf.state = PreCandidate
		rf.leader = -1
	} else {
		rf.reset(rf.term + 1)
		rf.state = Candidate
		rf.votes[rf.me] = 1
		rf.vote = rf.me
	}
	DebugPrint("%d become %s candidate, %v\n", rf.me, getMsgName(msgType), rf.lastElection)
	return term
}

func (rf *Raft) getUnsendEntries(since int) ([]Entry, int) {
	if since > rf.raftLog.GetLastIndex() {
		return []Entry{}, rf.raftLog.GetLastIndex()
	}
	Entries := rf.raftLog.GetEntries(since)
	return Entries, since - 1
}

func (rf *Raft) createMessage(to int, msgType MessageType) AppendMessage {
	var msg AppendMessage
	msg.Term = rf.term
	msg.From = rf.me
	msg.To = to
	msg.MsgType = msgType
	return msg
}

type Pair struct {
	value int
	idx	  int
}

type Pairs []Pair

func (p Pairs) Len() int {
	return len(p)
}

func (p Pairs) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type SortByFirst struct { Pairs }

func (p SortByFirst) Less(i, j int) bool {
	return p.Pairs[i].value > p.Pairs[j].value
}

func calculateHash(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := h.Sum(nil)
	return hex.EncodeToString(hashed)
}



//calculateBlockHash returns the hash of all block information
func calculateBlockHash(Term int, Index int, idx int, Timestamp string,BPM string, prevHash string, validator string) string {
record := string(Index) + Timestamp + string(BPM) + prevHash + string(idx) + string(Term) + validator
return calculateHash(record)
}

// func (rf *Raft) propose(data interface{}, idx int) {
// 	logNum := rf.raftLog.GetLastIndex() + 1
// 	rf.raftLog.Append(Entry{data, rf.term, logNum, idx})
// 	rf.broadcast()
// }
func (rf *Raft) proposeNew(data interface{}, idx int, validator int) {
	logNum := rf.raftLog.GetLastIndex() + 1
	t := time.Now().String()
	prevHash := rf.raftLog.GetLastHash()
	hash := calculateBlockHash(rf.term,logNum,idx,t,"0",rf.raftLog.GetLastHash(), string(validator))
	rf.raftLog.Append(Entry{data, rf.term, logNum, idx,t, "0", hash, rf.raftLog.GetLastHash(),validator})

	fmt.Printf("\nBlock Details:\nCurrent Term:	%d\nIndex:	%d\nTimeStamp:	%s\nData:	%s\nHash:	%s\nPrevHash:	%s\nValidator:	%d\n",rf.term, logNum,t, "0", hash, prevHash,validator)
	rf.broadcast()
}

func (rf *Raft) broadcast() {
	DebugPrint("%d: BeginSend append entries\n", rf.me)
	defer DebugPrint("%d: EndSend append entries:\n", rf.me)
	//msg := rf.createMessage(0, MsgAppend)
	for id, pr := range rf.clients {
		if id != int(rf.me) {
			rf.appendMore(id)
			DebugPrint("%d: broadcast append to %d since %d\n", rf.me, id, pr.next)
			//msg.To = id
			//msg.Entries, msg.PrevLogIndex = rf.getUnsendEntries(pr.next)
			//msg.Commited = rf.raftLog.commited
			//msg.PrevLogTerm = rf.raftLog.Entries[msg.PrevLogIndex].Term
			//rf.clients[msg.To].AppendAsync(&msg)
		}
	}
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) bcastHeartbeat(msg AppendMessage) {
	for idx, pr := range rf.clients {
		if idx != rf.me {
			msg.To = idx
			msg.Commited = MinInt(pr.matched, rf.raftLog.commited)
			//DebugPrint("%d: broadcast heartbeat to %d, commit to min(%d, %d)\n", rf.me, idx, pr.matched, rf.raftLog.commited)
			rf.clients[msg.To].AppendAsync(&msg)
		}
	}
}


func (rf *Raft) maybeLose() {
	succeed := 0
	for idx, v := range rf.clients {
		if idx == rf.me {
			succeed ++
		} else if v.active {
			succeed ++
			rf.clients[idx].active = false
		} else {
			//DebugPrint("%d lose contact of %d.\n", rf.me, idx)
		}
	}
	if succeed <= len(rf.clients) / 2 {
		rf.becomeFollower(rf.term, -1)
	}
}

func (rf *Raft) maybeChange() {
	state := HardState{rf.term, rf.vote, rf.raftLog.commited, rf.raftLog.Size()}
	if state != rf.prevState{
		start := time.Now()
		rf.persist()
		rf.prevState = state
		calcRuntime(start, "maybeChange")
	}
}

func (rf *Raft) campaign(msgType MessageType) {
	DebugPrint("%d begin %s campagin at term:%d, state:%d, log len:%d\n", rf.me, getMsgName(msgType), rf.term, rf.state, len(rf.raftLog.Entries))
	fmt.Printf("\nThe server %d begin %s campagin at term:%d, log len:%d\n", rf.me, getMsgName(msgType), rf.term, len(rf.raftLog.Entries))
	term := rf.becomeCandidate(msgType)
	rf.votes[rf.me] = 1
	lastLogIndex := rf.raftLog.GetLastIndex()
	lastLogTerm := rf.raftLog.GetLastTerm()
	//fmt.Printf("\ntotal peers:	%d\n",len(rf.peers))

	for idx, _ := range rf.peers {
		if idx != rf.me {
			fmt.Printf("\nCandidate %d Collecting pre-vote from server: %d\n", rf.me,idx)
			var msg RequestVoteArgs
			msg.MsgType = msgType
			msg.From = rf.me
			msg.Term = term
			msg.LastLogIndex = lastLogIndex
			msg.LastLogTerm = lastLogTerm
			msg.To = idx
			rf.clients[idx].VoteAsync(msg)
		}
	}
	// for idx := range rf.votes {
	// 	if(rf.votes[idx] == 1){
	// 		counterVotes++
	// 	}
	// // }

	// fmt.Printf("\nvotes total:%d", counterVotes)
}

func (rf *Raft) passed_election_time(electionTimeout int32, now time.Time) bool {
	return rf.lastElection.Add(time.Duration(electionTimeout) * time.Millisecond).Before(now)
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.tick_leader()
	} else {
		rf.tick_follower()
	}
	rf.maybeChange()
	rf.mu.Unlock()
}

func (rf *Raft) tick_leader() {
	now := time.Now()
	if rf.passed_election_time(rf.electionTimeout, now) {
		rf.lastElection = now
		rf.maybeLose()
		return
	} else if rf.lastHeartBeat.Add(time.Duration(200) * time.Millisecond).Before(now) {
		rf.lastHeartBeat = now
		msg := rf.createMessage(0, MsgHeartbeat)
		rf.bcastHeartbeat(msg)
	}
}

func (rf *Raft) tick_follower() {
	now := time.Now()
	if rf.passed_election_time(rf.rdElectionTimeout, now) {
		rf.lastElection = now
		rf.campaign(MsgRequestPrevote)
	}
}

func (rf *Raft) step() {
	rf.recoverFromSnapshot(rf.persister.ReadSnapshot())
	defer DebugPrint("Stop Raft: %d\n", rf.me)
	for {
		rf.tick()
		select {
		case msg := <- rf.voteChan : {
			if rf.state != Candidate && rf.state != PreCandidate && rf.leader != -1 {
				break
			}
			rf.mu.Lock()
			rf.handleVoteReply(&msg)
			rf.mu.Unlock()
		}
		case msg := <- rf.msgChan : {
			rf.mu.Lock()
			rf.handleAppendReply(&msg)
			rf.mu.Unlock()
		}
		case <- rf.stopChan: {
			return
		}
		case <-time.After(time.Duration(40) * time.Millisecond): {
			break
		}
		}
	}
}

//var eletionTimes [2000]bool

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DebugPrint("%d : start a Raft instance\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	e := Entry{nil, 0, 0, 0, "", "", calculateHash("0"+"0"+"0"+""+""+""+"0"), "",0}
	rf.raftLog = UnstableLog{
		[]Entry{e},
		0, 0, 1, nil,
	}
	rf.prevState = HardState{0, -1, 0, 1}
	rf.term = 0
	rf.vote = -1
	rf.electionTimeout = 800
	rf.rdElectionTimeout = 800 + 40 * int32(me)
	rf.lastHeartBeat = time.Now()
	rf.lastElection = time.Now()
	rf.applySM = applyCh
	rf.stop = true
	rf.msgChan = make(chan AppendReply, 2000)
	rf.voteChan = make(chan RequestVoteReply, 1000)
	rf.votes = make([]int, len(rf.peers))
	rf.stopChan = make(chan bool)
	// Your initialization code here.
	rf.becomeFollower(0, -1)
	rf.recoverFromPersist(persister.ReadRaftState())
	rf.clients = make([]RaftClient, len(rf.peers))
	for idx := range rf.clients {
		if idx != rf.me {
			rf.clients[idx].id = idx
			rf.clients[idx].peer = rf.peers[idx]
			rf.clients[idx].raft= rf
			rf.clients[idx].Start()
		}
	}

	go rf.step()
	return rf
}
