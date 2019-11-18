package raft

import (
	"time"
	"sort"
)

func (rf *Raft) AppendEntries(args *AppendMessage, reply* AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.To = args.From
	reply.From = rf.me
	reply.MsgType = getResponseType(args.MsgType)
	reply.Id = args.Id

	if !rf.checkAppend(args.From, args.Term, args.MsgType) {
		DebugPrint("%d reject (%s) from leader: %d, term: %d, leadder term: %d\n", rf.me, getMsgName(args.MsgType),
			args.From, rf.term, args.Term)
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = 0
		return
	}

	if rf.stop {
		return
	}

	rf.leader = args.From
	rf.lastElection = time.Now()
	rf.state = Follower
	//DebugPrint("%d(%d) access msg from %d(%d)\n", rf.me, rf.term,
	//		args.From, args.Term)
	if args.MsgType == MsgHeartbeat {
		//DebugPrint("%d(commit: %d, applied: %d, total: %d) access Heartbeat from %d(%d) to %d\n", rf.me, rf.raftLog.commited,
		//	rf.raftLog.applied, rf.raftLog.Size(), args.From, args.Commited, args.To)
		rf.handleHeartbeat(args, reply)
	} else if args.MsgType == MsgAppend {
		rf.handleAppendEntries(args, reply)
		//DebugPrint("%d(%d) access append from %d(%d) to %d\n", rf.me, rf.raftLog.commited,
		//	args.From, args.Commited, args.To)
	} else if args.MsgType == MsgSnapshot {
		rf.handleAppendSnapshot(args, reply)
		return
	}
	if rf.raftLog.applied < rf.raftLog.commited {
		entries := rf.raftLog.GetUnApplyEntry()
		if len(entries) > 0 {
			DebugPrint("%d apply entries from %d to %d, which commited is %d, applied is %d\n",
				rf.me, entries[0].Index,entries[len(entries) - 1].Index, rf.raftLog.commited, rf.raftLog.applied)
		}
		for _, e := range entries {
			m := rf.createApplyMsg(e)
			if m.CommandValid {
				DebugPrint("%d apply an entry of log[%d]=data[%d]\n", rf.me, e.Index, m.CommandIndex)
				rf.applySM <- m
			}
			rf.raftLog.applied = e.Index
			//if rf.raftLog.applied > rf.raftLog.commited {
			//	DebugPrint("%d, commited: %d\n", rf.me, rf.raftLog.commited)
			//	panic("APPLY a bigger index entry")
			//}
		}
	}
	rf.maybeChange()
}


func (rf *Raft) handleAppendSnapshot(args *AppendMessage, reply* AppendReply) {
	snap := args.Snap
	if args.Snap.Index > rf.raftLog.commited && rf.applySnapshot(&snap) {
		reply.Success = true
		reply.Commited = snap.Index
		reply.Term = args.Term
		DebugPrint("%d(%d) apply snapshot from %d(%d) success\n", rf.me, rf.raftLog.commited,
			args.From, snap.Index)
	} else {
		reply.Success = true
		reply.Commited = rf.raftLog.commited
		reply.Term = args.Term
		DebugPrint("%d(%d) apply snapshot from %d(%d) failed\n", rf.me, rf.raftLog.commited,
			args.From, snap.Index)
	}
}

func (rf *Raft) handleAppendEntries(args *AppendMessage, reply *AppendReply)  {
	reply.MsgType = MsgAppendReply
	index := rf.raftLog.GetLastIndex()
	if args.PrevLogIndex > index {
		DebugPrint("%d(index: %d, %d) reject append entries from %d(prev index: %d)\n",
			rf.me, index, rf.term, args.From, args.PrevLogIndex)
		reply.Success = false
		//reply.Commited = index - 1
		reply.Commited = rf.raftLog.commited
		reply.Term = rf.term
		return
	}
	if rf.raftLog.MatchIndexAndTerm(args.PrevLogIndex, args.PrevLogTerm) {
		lastIndex := args.PrevLogIndex + len(args.Entries)
		conflict_idx := rf.raftLog.FindConflict(args.Entries)
		if conflict_idx == 0 {
		} else if conflict_idx <= rf.raftLog.commited {
			DebugPrint("%d(index: %d, %d) conflict append entries from %d(prev index: %d)\n",
				rf.me, index, rf.term, args.From, args.PrevLogIndex)
			return
		} else {
			from := conflict_idx - args.PrevLogIndex - 1
			ed := len(args.Entries) - 1
			if ed >= 0 {
				DebugPrint("%d access append from %d, append entries from %d to %d, prev: %d, conflict: %d\n",
					rf.me, args.From, from, ed, args.PrevLogIndex, conflict_idx)
			}
			for _, e:= range args.Entries[from:] {
				rf.raftLog.Append(e)
			}
		}
		DebugPrint("%d commit to %d -> min(%d, %d) all msg: %d -> %d, preindex :%d\n", rf.me, rf.raftLog.commited,
			args.Commited, lastIndex, index, rf.raftLog.Size(), args.PrevLogIndex)
		rf.raftLog.MaybeCommit(MinInt(args.Commited, lastIndex))
		reply.Term = rf.term
		reply.Commited = lastIndex
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.term
		reply.Commited = args.PrevLogIndex - 1
		if rf.raftLog.GetLastIndex() > 2 + rf.raftLog.commited {
			reply.Commited = rf.raftLog.commited
		}
		DebugPrint("%d(commit  %d) reject append entries from %d(prev index: %d, term: %d)\n",
			rf.me, rf.raftLog.commited, args.From, args.PrevLogIndex, args.PrevLogTerm)
		//DebugPrint("%d(index: %d, term: %d) %d reject append entries from %d(prev index: %d, term: %d)\n",
		//	rf.me, e.Index, e.Term, rf.raftLog.commited, args.From, args.PrevLogIndex, args.PrevLogTerm)
	}
}

func (rf *Raft) handleHeartbeat(msg *AppendMessage, reply *AppendReply)  {
	reply.Success = true
	reply.Term = MaxInt(rf.term, reply.Term)
	reply.Commited = rf.raftLog.GetLastIndex()
	reply.MsgType = MsgHeartbeatReply
	rf.term = msg.Term
	if rf.raftLog.MaybeCommit(msg.Commited) {
		DebugPrint("%d commit to %d, log length: %d, last index:%d leader : %d\n",
			rf.me, rf.raftLog.commited, rf.raftLog.Size(), rf.raftLog.GetLastIndex(), msg.From)
	}
}


func (rf *Raft) handleAppendReply(reply* AppendReply) {
	//DebugPrint("%d handleAppendReply from %d at %v\n", rf.me, reply.From, start)
	if !rf.checkAppend(reply.From, reply.Term, reply.MsgType) {
		return
	}
	if rf.leader != rf.me || rf.state != Leader{
		return
	}
	if rf.stop {
		return
	}
	pr := &rf.clients[reply.From]
	pr.active = true
	if reply.MsgType == MsgHeartbeatReply {
		if pr.matched < rf.raftLog.GetLastIndex() && pr.PassAppendTimeout() {
			rf.appendMore(reply.From)
		} else {
			//DebugPrint("%d skip append to %d because last append time: %v\n", rf.me, reply.From, pr.lastAppendTime)
		}
		//DebugPrint("%d access HeartbeatReply from %d(matched: %d, %d)\n", rf.me, reply.From,
		//	pr.matched, rf.raftLog.GetLastIndex())
		return
	} else if reply.MsgType == MsgSnapshotReply {
		DebugPrint("%d access Snapshot Reply from %d(matched: %d, %d)\n", rf.me, reply.From,
			pr.matched, rf.raftLog.GetLastIndex())
		panic("NO MSGSNAPSHOTREPLY\n")
		return
	}
	if !reply.Success {
		//DebugPrint("%d(%d) handleAppendReply failed, from %d(%d). which matched %d\n",
		//	rf.me, rf.raftLog.commited, reply.From, reply.Commited, pr.matched)
		if reply.Commited + 1 < pr.next {
			//if reply.Commited + 1 < pr.next && reply.Commited > pr.matched {
			if reply.Commited < pr.matched && !pr.PassAppendTimeout() {
				return
			}
			pr.next = reply.Commited + 1
			rf.appendMore(reply.From)
		}
	} else {
		//DebugPrint("%d: %d handleAppendReply from %d(%d), commit log from %d to %d\n",
		//	reply.Id, rf.me, reply.From, reply.Term, pr.matched, reply.Commited)

		if pr.matched < reply.Commited {
			pr.matched = reply.Commited
			pr.next = reply.Commited + 1
		}
/*		if reply.Commited <= rf.raftLog.commited {
			return
		}*/
		commits := make([]int, len(rf.peers))
		for i, p := range rf.clients {
			if i == rf.me {
				commits[i] = rf.raftLog.GetLastIndex()
			} else {
				commits[i] = p.matched
			}
		}
		sort.Ints(commits)
		quorum := len(rf.peers) / 2
		//DebugPrint("%d receive a msg commit : %d from %d\n", rf.me, reply.Commited, reply.From)

		if rf.raftLog.commited < commits[quorum] {
			DebugPrint("%d commit %d, to commit %d, apply %d, all: %d\n",
			rf.me, rf.raftLog.commited, commits[quorum], rf.raftLog.applied,
				rf.raftLog.size)
			rf.raftLog.commited = commits[quorum]
			entries := rf.raftLog.GetUnApplyEntry()
			for _, e := range entries {
				m := rf.createApplyMsg(e)
				if e.Index != rf.raftLog.applied + 1 {
					DebugPrint("%d APPLY ERROR! %d, %d\n", rf.me, e.Index, rf.raftLog.applied)
					panic("APPLY ERROR")
				}
				if m.CommandValid {
					rf.applySM <- m
					DebugPrint("%d apply an entry in leader, log[%d]=data[%d]\n", rf.me, e.Index, m.CommandIndex)
				}
				rf.raftLog.applied = e.Index
			}
			DebugPrint("%d apply message\n", rf.me)
			rf.maybeChange()
		}
		if pr.PassAppendTimeout() {
			rf.appendMore(reply.From)
		}
	}
}


func (rf *Raft) applySnapshot(s *Snapshot) bool {
	if s == nil {
		return false
	}
	start := time.Now()
	defer calcRuntime(start, "ApplySnapshot")
	DebugPrint("%d apply snapshot from %d, %d, %d\n", rf.me, rf.term, rf.vote, rf.raftLog.commited)
	var msg ApplyMsg
	msg.CommandValid = false
	msg.Snap = s
	msg.LogIndex = s.Index
	if rf.raftLog.snapshot == nil || s.Index > rf.raftLog.snapshot.Index {
		rf.applySM <- msg
	} else {
		return false
	}
	if !rf.raftLog.SetSnapshot(s) {
		DebugPrint("%d snapshot error, log size %d, snapshot index: %d, self snapshot index: %d\n",
			rf.me, rf.raftLog.Size(), s.Index, rf.raftLog.snapshot.Index)
		panic("APPLY SNAPSHOT ERROR")
		return false
	}
	rf.persister.SaveStateAndSnapshot(rf.getRaftStateData(), s.Bytes())
	DebugPrint("%d end snapshot from %d, %d, %d\n", rf.me, rf.term, rf.vote, rf.raftLog.commited)
	return true
}

func (rf *Raft) checkAppend(from int, term int, msgType MessageType) bool {
	if term > rf.term {
		rf.becomeFollower(term, from)
	} else if term < rf.term {
		DebugPrint("==================!ERROR!======append message(%s) from %d(%d) to %d(%d) can not be reach, leader: %d\n",
			getMsgName(msgType), from, term, rf.me, rf.term, rf.leader)
		return false
	}
	return true
}
