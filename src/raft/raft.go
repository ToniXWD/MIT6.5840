package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term int
	Cmd  interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 500

	ElectTimeOutCheckInterval = time.Duration(350) * time.Millisecond // 检查是否超时的间隔
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// 以下不是Figure 2中的field
	timeStamp time.Time // 记录收到消息的时间(心跳或append)
	role      int

	muVote    sync.Mutex // 保护投票数据
	voteCount int
}

func (rf *Raft) Print() {
	DPrintf("raft%v:{currentTerm=%v, role=%v, votedFor=%v}\n", rf.me, rf.currentTerm, rf.role, rf.votedFor)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// 旧的term
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的term: %v,\n\targs= %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}

	// 代码到这里时, args.Term >= rf.currentTerm

	if args.Term > rf.currentTerm {
		// 已经是新一轮的term, 之前的投票记录作废
		rf.votedFor = -1
	}

	// at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 首先确保是没投过票的
		if args.Term > rf.currentTerm ||
			(args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) {
			// 2. If votedFor is null or candidateId, and candidate’s log is least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.role = Follower
			rf.timeStamp = time.Now()

			rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
			return
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票: 已投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
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

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		// 易错点: 函数调用的间隙被修改了
		return false
	}

	if reply.Term > rf.currentTerm {
		// 已经是过时的term了
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}

	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		rf.role = Leader
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}

	rf.muVote.Unlock()
}

func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm += 1       // 自增term
	rf.role = Candidate       // 成为候选人
	rf.votedFor = rf.me       // 给自己投票
	rf.voteCount = 1          // 自己有一票
	rf.timeStamp = time.Now() // 自己给自己投票也算一种消息

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			DPrintf("vote for self : Raft[%d]", rf.me)
			continue
		}
		go rf.collectVote(server, args)
	}
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// 新leader发送的第一个消息
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// 这是来自旧的leader的消息
		// 1. Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// 代码执行到这里就是 args.Term >= rf.currentTerm 的情况

	// 不是旧 leader的话需要记录访问时间
	rf.timeStamp = time.Now()

	if args.Term > rf.currentTerm {
		// 新leader的第一个消息
		rf.currentTerm = args.Term // 更新iterm
		rf.votedFor = -1           // 易错点: 更新投票记录为未投票
		rf.role = Follower
	}

	if args.Entries == nil {
		// 心跳函数
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	}
	if args.Entries != nil &&
		(args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// 校验PrevLogIndex和PrevLogTerm不合法
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// TODO: 补充apeend的业务

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleHeartBeat(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	sendArgs := *args // 复制一份args结构体, (可能有未知的错误)
	ok := rf.sendAppendEntries(serverTo, &sendArgs, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if sendArgs.Term != rf.currentTerm {
		// 函数调用间隙值变了
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("server %v 旧的leader收到了心跳函数中更新的term: %v, 转化为Follower\n", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v start sending heartbeats\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		// if the server is dead or is not the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			// 不是leader则终止心跳的发送
			return
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers) && i != rf.me; i++ {
			go rf.handleHeartBeat(i, args)
		}

		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	r := rand.New(rand.NewSource(int64(rf.me)))
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		timeout := int(r.Float64()*(500.0) + ElectTimeOutBase)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.timeStamp) > time.Duration(timeout)*time.Millisecond {
			// 超时
			go rf.Elect()
		}
		rf.mu.Unlock()
		time.Sleep(ElectTimeOutCheckInterval)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeStamp = time.Now()
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
