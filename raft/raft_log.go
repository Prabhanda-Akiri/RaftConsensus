package raft

import "fmt"


type Entry struct {
	//Data []byte
	Data interface{}
	Term int
	Index int
	DataIndex int
	Timestamp string
	BPM       string
	Hash      string
	PrevHash  string
	Validator int
}

type UnstableLog struct {
	Entries		[]Entry
	commited	int
	applied		int
	size		int
	snapshot    *Snapshot
}

func (log *UnstableLog) Size() int {
	return log.size
}

func (log* UnstableLog) GetSnapshot() *Snapshot {
	return log.snapshot
}

func (log* UnstableLog) SetSnapshot(snapshot *Snapshot) bool {
	if snapshot == nil {
		return false
	}
	prevIndex := 0
	if log.snapshot != nil {
		prevIndex = log.snapshot.Index + 1
		if prevIndex > snapshot.Index {
			return false
		}
	}
	var entries []Entry
	if log.size - prevIndex > 0 {
		entries = make([]Entry, log.size - prevIndex)
		copy(entries, log.Entries[:log.size - prevIndex])
	}
	log.snapshot = snapshot
	log.size = snapshot.Index + 1
	if log.commited < snapshot.Index {
		log.commited = snapshot.Index
	}
	if log.applied < snapshot.Index {
		log.applied = snapshot.Index
	}
	prevLogIndex := snapshot.Index
	for _, e := range entries {
		if e.Index <= log.snapshot.Index {
			continue
		}
		log.Append(e)
		if log.GetLastIndex() != prevLogIndex + 1 {
			fmt.Printf("_______entry index: %d, last index: %d, previndex: %d\n", e.Index, log.GetLastIndex(), prevLogIndex)
			panic("Snapshot error")
		}
		prevLogIndex = log.GetLastIndex()
	}
	return true
}

func (log *UnstableLog) MatchIndexAndTerm(index int, term int) bool {
	if index >= log.size {
		return false
	}
	if log.snapshot != nil {
		if index < log.snapshot.Index {
			return false
		}
		if index == log.snapshot.Index {
			return term == log.snapshot.Term
		}
		return log.Entries[index - log.snapshot.Index - 1].Term == term
	}
	return log.Entries[index].Term == term
}

func (log *UnstableLog) GetEntry(idx int) *Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
		if idx == log.snapshot.Index {
			return &Entry{nil, log.snapshot.Term, log.snapshot.Index, log.snapshot.DataIndex, log.snapshot.Timestamp, log.snapshot.BPM, log.snapshot.Hash, log.snapshot.PrevHash, log.snapshot.Validator}
		}
	}
	if idx - prevSize < 0 || idx - prevSize >= len(log.Entries) {
		fmt.Printf("__________GetEntry index: %d, prevSize: %d, len of entries: %d\n", idx, prevSize, len(log.Entries))
		panic("out of range")
	}
	return &log.Entries[idx - prevSize]
}

func (log *UnstableLog) GetDataIndex() int {
	return log.GetEntry(log.size - 1).DataIndex
}

func (log *UnstableLog) GetLastIndex() int {
	return log.GetEntry(log.size - 1).Index
}

func (log *UnstableLog) GetLastTerm() int {
	return log.GetEntry(log.size - 1).Term
}

func (log *UnstableLog) GetLastValidator() int {
	return log.GetEntry(log.size - 1).Validator
}

func (log *UnstableLog) GetLastHash() string {
	return log.GetEntry(log.size - 1).Hash
}

func (log *UnstableLog) Append(e Entry) {
	idx := e.Index
	if log.snapshot != nil {
		idx -= log.snapshot.Index + 1
	}
	if idx > len(log.Entries) {
		fmt.Printf("Append an error entry, which index is %d, idx is %d, bigger than last index: %d, when snapshot index is %d\n",
			e.Index, idx, log.Entries[len(log.Entries) - 1].Index, log.snapshot.Index)
		panic("===APPEND ERROR==========")
	}
	if idx >= len(log.Entries) {
		log.Entries = append(log.Entries, e)
	} else {
		log.Entries[idx] = e
	}
	if idx > 0 && e.Index != log.Entries[idx - 1].Index + 1 {
		fmt.Printf("append error, append entry {%d} in %d,which prev entry index is %d. snapshot index is %d\n",
			e.Index, idx, log.Entries[idx - 1].Index, log.snapshot.Index)
		panic("===APPEND ERROR==========")
	}
	log.size = e.Index + 1
	if log.snapshot == nil && log.size > len(log.Entries) {
		fmt.Printf("append error, append entry {%d} in %d,which log size is %d, entries len: %d\n",
			e.Index, idx, log.size, len(log.Entries))
		panic("===APPEND ERROR==========")
	}
}

func (log *UnstableLog) FindConflict(entries []Entry) int {
	for _, e := range entries {
		if e.Index >= log.size || !log.MatchIndexAndTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

func (log *UnstableLog) IsUpToDate(Index int, Term int) bool {
	ans := false
//	return Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex())
	if Term > log.GetLastTerm() || (Term == log.GetLastTerm() && Index >= log.GetLastIndex()) {
		ans = true
	}
	//fmt.Printf("len: %d, term: %d, last term: %d, index: %d, lastIndex: %d, result: %t\n", len(log.Entries), Term, log.GetLastTerm(),
	//	Index, log.GetLastIndex(), ans)
	return ans
}

func (log *UnstableLog) GetUnApplyEntry() []Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}
	if log.commited == log.applied {
		return []Entry{}
	}
	entries := make([]Entry, log.commited - log.applied)
	copy(entries, log.Entries[log.applied + 1 - prevSize : log.commited + 1 - prevSize])
	return entries
}

func (log *UnstableLog) GetEntries(since int) []Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}
	entries := make([]Entry, log.size - since)
	copy(entries, log.Entries[since - prevSize : log.size - prevSize])
	return entries
}

func (log *UnstableLog) GetUnstableEntries() []Entry {
	prevSize := 0
	if log.snapshot != nil {
		prevSize = log.snapshot.Index + 1
	}
	sz := log.size - prevSize
	entries := make([]Entry, sz)
	copy(entries, log.Entries[: sz])
	return entries
}

func (log *UnstableLog) MaybeCommit(index int) bool {
	if index > log.commited && index < log.size{
		log.commited = index
		return true
	}
	return false
}