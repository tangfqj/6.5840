package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Entry struct {
	Command      interface{}
	CommandTerm  int
	CommandIndex int
}

type NodeState string

const (
	Leader    NodeState = "Leader"
	Follower            = "Follower"
	Candidate           = "Candidate"
)

type Raft struct {
	mu sync.RWMutex // Lock to protect shared access to this peer's state
	// Use RWMutex for better performance
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state NodeState

	currentTerm int
	votedFor    int
	logs        []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan raftapi.ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// 3A
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) ChangeState(newState NodeState) {
	if rf.state == newState {
		return
	}
	rf.state = newState
	switch newState {
	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	case Candidate:
	case Follower:
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.heartbeatTimer.Stop()
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	// election restrction (section 5.4.1)
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Code for 3B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	newLogIndex := rf.getLastLog().CommandIndex + 1
	rf.logs = append(rf.logs, Entry{
		Command:      command,
		CommandIndex: newLogIndex,
		CommandTerm:  rf.currentTerm,
	})
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex+1
	rf.BroadcastHeartbeat(false)
	return newLogIndex, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex < rf.getFirstLog().CommandIndex {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// if an existing entry conflict with a new one (same index but different term), delete it and all that follow it
	if !rf.isLogMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().CommandIndex
		// find the first index of conflicting term
		if lastLogIndex < args.PrevLogIndex {
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex+1, -1
		} else {
			firstLogIndex := rf.getFirstLog().CommandIndex
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstLogIndex].CommandTerm
			index := args.PrevLogIndex - 1
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].CommandTerm == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	// append new entries not in the log
	firstLogIndex := rf.getFirstLog().CommandIndex
	for index, entry := range args.Entries {
		if entry.CommandIndex-firstLogIndex >= len(rf.logs) ||
			rf.logs[entry.CommandIndex-firstLogIndex].CommandTerm != entry.CommandTerm {
			rf.logs = shrinkEntries(append(rf.logs[:entry.CommandIndex-firstLogIndex], args.Entries[index:]...))
			break
		}
	}

	newCommitIndex := min(args.LeaderCommit, rf.getLastLog().CommandIndex)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) StartElection() {
	rf.votedFor = rf.me
	rf.persist()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().CommandIndex,
		LastLogTerm:  rf.getLastLog().CommandTerm,
	}
	grantedVotes := 1
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteArgs %v from {Node %v}", rf.me, reply, peer)
				if args.Term == rf.currentTerm && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes++
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes", rf.me)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(heartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if heartbeat {
			go rf.replicateOnceRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicateOnceRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex >= rf.getFirstLog().CommandIndex {
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if args.Term == rf.currentTerm && rf.state == Leader {
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						// current server is no longer leader
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					} else if reply.Term == rf.currentTerm {
						// decrease nextIndex and retry
						rf.nextIndex[peer] = reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							// binary search
							firstLogIndex := rf.getFirstLog().CommandIndex
							lo, hi := firstLogIndex, args.PrevLogIndex-1
							for lo < hi {
								mid := (lo + hi + 1) / 2
								if rf.logs[mid-firstLogIndex].CommandTerm <= reply.ConflictTerm {
									lo = mid
								} else {
									hi = mid - 1
								}
							}
							if rf.logs[lo-firstLogIndex].CommandTerm == reply.ConflictTerm {
								rf.nextIndex[peer] = lo
							}
						}
					}
				} else {
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// get the index of the entry with the highest entry that is known to be replicated on majority
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatch(newCommitIndex, rf.currentTerm) {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}
func (rf *Raft) needReplicate(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// check the logs of peer is behind the leader
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().CommandIndex
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicate(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOnceRound(peer)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		// apply log entries to state machine
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().CommandIndex, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		// send apply message to applyCh for replica
		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.CommandIndex,
				CommandTerm:  entry.CommandTerm,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.persist()
			// start election
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		mu:             sync.RWMutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().CommandIndex+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(peer)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}
