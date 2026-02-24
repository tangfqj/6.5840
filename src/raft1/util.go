package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const ElectionTimeout = 1000
const HeartbeatTimeout = 125

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var GlobalRand = &LockedRand{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}

func RandomElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) logIndex(absoluteIndex int) int {
	return absoluteIndex - rf.getFirstLog().CommandIndex
}

func (rf *Raft) isLogMatch(index, term int) bool {
	return index <= rf.getLastLog().CommandIndex && term == rf.logs[index-rf.getFirstLog().CommandIndex].CommandTerm
}

func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.CommandTerm || (term == lastLog.CommandTerm && index >= lastLog.CommandIndex)
}

func shrinkEntries(entries []Entry) []Entry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().CommandIndex
	entries := make([]Entry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].CommandTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}
