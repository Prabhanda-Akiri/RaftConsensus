package raft

import "bytes"
import "labgob"

type Snapshot struct {
	Index         int
	DataIndex     int
	Term          int
	Data          []byte
	Timestamp string
	BPM       string
	Hash      string
	PrevHash  string
	Validator int
}


func MakeSnapshot(data []byte) *Snapshot {
	s := &Snapshot{}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&s.Index)
	d.Decode(&s.DataIndex)
	d.Decode(&s.Term)
	d.Decode(&s.Data)
	d.Decode(&s.Timestamp)
	d.Decode(&s.BPM)
	d.Decode(&s.Hash)
	d.Decode(&s.PrevHash)
	d.Decode(&s.Validator)
	return s
}

func (s *Snapshot) Bytes() []byte {
	w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(s.Index)
	e.Encode(s.DataIndex)
	e.Encode(s.Term)
	e.Encode(s.Data)
	e.Encode(s.Timestamp)
	e.Encode(s.BPM)
	e.Encode(s.Hash)
	e.Encode(s.PrevHash)
	e.Encode(s.Validator)
    return w.Bytes()
}
