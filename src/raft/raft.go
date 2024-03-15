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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)
const (
	MinElectionTimeout = 200
	MaxElectionTimeout = 400
	HeardBeatTimeout   = 120
)

type logEntry struct {
	Term    int
	Command interface{}
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	currentTerm     int                 // 服务器已知的最新任期(在服务器首次启动时设为0,单调增)
	voteFor         int                 // 当前任期内给 candidateId 投了赞成，如果没有给任何候选人投赞成 则为空.根据定义,切换到新任期时置为-1
	log             []logEntry          // 每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	commitIndex     int                 // 已知已提交的最高的日志条目的索引（初始值为0，单调递增
	lastApplied     int                 // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	nextIndex       []int               // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex      []int               // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	RaftStatus      RaftStatus          // Node所处状态
	heartbeatsTimer *time.Timer         // 在非Leader模式时停止
	electionTimer   *time.Timer         // 在leader模式时停止
}

// 随机返回一个以毫秒为单位的时间段
func randomDuration() time.Duration {
	return time.Duration(rand.Intn(MaxElectionTimeout-MinElectionTimeout)+MinElectionTimeout) * time.Millisecond
}

// 重置选举时间
func (rf *Raft) ResetElectionTimeout() {
	rf.electionTimer.Reset(randomDuration())
}

// 返回最后一条日志的索引(第一条日志索引为1),若没有日志返回-1
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return -1
	} else {
		return len(rf.log)
	}
}

// 获得倒数第二条日志的任期
func (rf *Raft) getPrevLogTerm() int {
	if len(rf.log) >= 2 {
		return rf.log[len(rf.log)-2].Term
	} else {
		return -1
	}
}

// 获得倒数第二条日志的索引
func (rf *Raft) getPrevLogIndex() int {
	if len(rf.log) >= 2 {
		return len(rf.log) - 1
	} else {
		return -1
	}
}

// 返回最后一条日志的任期号,若没有日志返回-1
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

// 产生一个RequestVoteArgs
func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	return args

}

func (rf *Raft) genHeartbeatAppendEntriesArgs() AppendEntriesArgs {
	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.getPrevLogIndex()
	args.PrevLogTerm = rf.getLastLogTerm()
	args.Entries = []logEntry{}
	args.LeaderCommit = rf.commitIndex
	return args
}

// campaign
// 监听心跳超时计时器和选举超时计时器
func (rf *Raft) ticker() {
	// tips: CampaignForVotes,BroadcastHeartbeat函数都是在加锁的情况下执行,因此要尽快返回,避免性能下降
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf(rf.me, "{Node %v} 选举计时器超时 ", rf.me)
			if rf.RaftStatus == Leader {
				panic("在leader状态选举")
			}
			// 选举流程
			// 1. 将自身切换为Candidate 状态
			// 2. 任期号+1
			// 3. 请求选票
			// 4. 重置选举超时计时器
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.CampaignForVotes() // 函数立即返回,内部耗时过程放入新goroutines
			rf.ResetElectionTimeout()
			rf.mu.Unlock()
		case <-rf.heartbeatsTimer.C:
			rf.mu.Lock()
			if rf.RaftStatus == Leader {
				rf.BroadcastHeartbeat() // 函数立即返回
				rf.heartbeatsTimer.Reset(HeardBeatTimeout * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

// 心跳广播
// 🔐🔐🔐🔐🔐🔐🔐🔐 在持有锁的状态下被调用
func (rf *Raft) BroadcastHeartbeat() {
	DPrintf(rf.me, "Term: %v | {Node %v} 开始广播心跳", rf.currentTerm, rf.me)
	args := rf.genHeartbeatAppendEntriesArgs()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			var reply AppendEntriesReply
			DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} AppendEntriesArgs: %v", rf.currentTerm, rf.me, peer, args)
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} AppendEntriesReply: %v", rf.currentTerm, rf.me, peer, reply)
				// todo
				// 后续要对AppendEntriesReply做详细处理,这里暂时只处理心跳
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.ChangeState(Follower)
					rf.voteFor = -1
				}
			} else {
				DPrintf(rf.me, "{Node %v} -> {Node %v} 心跳失败", rf.me, peer)
			}
			//}
		}(peer)
	}
}

// 请求其他Server 为自己投票
// 🔐🔐🔐🔐🔐🔐🔐🔐 在持有锁的状态下被调用
func (rf *Raft) CampaignForVotes() {
	args := rf.genRequestVoteArgs()
	rf.voteFor = rf.me
	currentVoteCount := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 1. 在对raft结构体加锁的前提下执行StartElection
		// 2. 锁在StartElection结束之后才释放
		// 3. 发送RequestVoteArgs的goroutine在开始的时候获取锁
		//  所有的发送RequestVoteArgs的goroutine要等到StartElection之后执行
		go func(peer int) {
			// 此时RequestVoteArgs还没发送
			// 在ticker的当前case结束后(释放了rf.mu)和在获取到rf.mu锁之间
			// 可能已经离开了Candidate状态,成为Follower或者Leader
			// 就没必要继续发送RequestVoteArgs了吗?

			// 收到RequestVoteReply后进行状态调整,这是一个RTT网络时延之后的事
			// 这个延迟远大于goroutine 发出多个请求,因此可以视为多个goroutine
			// 同时发出了RequestVoteArgs请求

			// 收到RequestVoteReply至少一个RTT,但是可能受到AppendEntriesArgs,和
			// RequestVoteArgs.这些可能会造成状态切换回Follower.

			// 但是raft中未讨论这种投票请求终止的情况,
			// 因为如果当前节点退回Follower,说明当前节点已经落后
			// 发出的RequestVoteArgs中的term落后,不会影响更新的Node

			var reply RequestVoteReply

			DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} RequestVoteArgs: %v", rf.currentTerm, rf.me, peer, args)
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 收到 RequestVoteReply
				DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} RequestVoteReply: %v", rf.currentTerm, rf.me, peer, reply)
				// 根据图4的状态转换,candidate有可能发现更高的任期号后转换成Follower
				// 给出选票的人看过RequestVoteArgs中的term后会更新自己的任期,reply.Term不可能小于rf.currentTerm
				// 在ticker超时处于Candidate状态,但是现在可能变回Follower,这会导致term增加,状态改变

				// 此时可能处于 Follower or Candidate or Leader
				// 1. Follower:收到更高Term currentTerm > args.Term
				// 2. Leader: 成为当选 currentTerm == args.Term
				// 3. Candidate: 新一轮选举开始 currentTerm > args.Term
				// 4. Candidate: 当前选举还在进行中
				// 只有case 4需要继续处理
				if args.Term == rf.currentTerm && rf.RaftStatus == Candidate {
					// 如果给当前Node 投了赞成
					if reply.VoteGranted {
						currentVoteCount += 1
						if currentVoteCount > len(rf.peers)/2 {
							DPrintf(rf.me, "Term: %v | {Node %v} 收到了半数选票当选 Leader", rf.currentTerm, rf.me)
							rf.ChangeState(Leader)
						}

						// 没有给自己投票, 两个因素1.任期 2.LastLog
						// 如果是因为任期原因
					} else if reply.Term > args.Term {
						// 只会发生一次,这段代码只有在Candid才能到达
						DPrintf(rf.me, "{Node %v} 在自己第%v 任期投票期间,从{Node %v} 的reply中发现了更高的任期 %v, 转变为Follower", rf.me, args.Term, peer, reply.Term)
						rf.ChangeState(Follower)
						rf.currentTerm = reply.Term
						rf.voteFor = -1
					}
				}
			} else {
				DPrintf(rf.me, "Term: %v | {Node %v} 未成功收到 {Node %v} 的RequestVoteReply", rf.currentTerm, rf.me, peer)
			}
		}(peer)
	}

}

func (rf *Raft) ChangeState(Rs RaftStatus) {
	// 根据论文图4
	switch Rs {
	case Leader:
		// 图2 Volatile state on leader,下面蓝字 Reinitialized after election
		//initialized to leader last log index + 1
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
		}
		//initialized to 0
		for i := 0; i < len(rf.matchIndex); i++ {
			rf.matchIndex[i] = 0
		}
		rf.RaftStatus = Leader
		// leader 关闭选举超时计时器
		rf.electionTimer.Stop()
		// leader 开启心跳超时计时器
		rf.heartbeatsTimer.Reset(HeardBeatTimeout)
	case Candidate:
		// 打开选举超时计时器
		rf.ResetElectionTimeout()
		// 关闭心跳超时计时器
		rf.heartbeatsTimer.Stop()
		rf.RaftStatus = Candidate
	case Follower:
		// 打开选举超时计时器
		rf.ResetElectionTimeout()
		// 关闭心跳超时计时器
		rf.heartbeatsTimer.Stop()
		rf.RaftStatus = Follower
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.RaftStatus == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 候选人ID
	LastLogIndex int //候选人的最后一条日志的索引值
	LastLogTerm  int // 候选人的最后一条日志的任期号

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号,以便候选人去更新自己的任期号,for candidate to update itself
	VoteGranted bool // 候选人赢得了此张选票时为真
}

type AppendEntriesArgs struct {
	Term         int        // 领导人任期
	LeaderId     int        // 领导人 ID 因此跟随者可以对客户端进行重定向
	PrevLogIndex int        // 倒数第二条日志的索引
	PrevLogTerm  int        // 倒数第二条日志的任期
	Entries      []logEntry //需要flower被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期号,for leader to update itself
	Success bool // flower在preveLogIndex位置的日志的任期是prevlogTerm, 则为true
}

// args 指向的日志至少和rf一样新
func (rf *Raft) IsLogOlderOrEqual(args *RequestVoteArgs) bool {
	raftLastLogTerm := rf.getLastLogTerm()
	raftLastLogIndex := rf.getLastLogIndex()
	//  args 的日志至少和rf的日志一样新
	if args.Term > raftLastLogTerm || (args.Term == raftLastLogTerm && args.LastLogIndex >= raftLastLogIndex) {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(rf.me, "{Node %v} 收到了心跳广播", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, "Term: %v | {Node %v} <- {Leader} AppendEntriesArgs: %v", rf.currentTerm, rf.me, args)
	defer DPrintf(rf.me, "Term: %v | {Node %v} -> {Leader} AppendEntriesReply: %v", rf.currentTerm, rf.me, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} else if args.Term == rf.currentTerm {
		// 同一任期不可能有两个Leader,当前节点Follower 或者 Candidate
		reply.Term, reply.Success = rf.currentTerm, false
		if rf.RaftStatus != Follower {
			rf.ChangeState(Follower)
		}
		rf.ResetElectionTimeout()
	} else if args.Term > rf.currentTerm {
		reply.Term, reply.Success = args.Term, false
		rf.currentTerm = args.Term
		rf.voteFor = -1
		if rf.RaftStatus != Follower {
			rf.ChangeState(Follower)
		}
		rf.ResetElectionTimeout()
	}
}

// 收到RequestVoteArgs的处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} RequestVoteArgs: %v", rf.currentTerm, rf.me, args.CandidateId, args)
	defer DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} RequestVotereply: %v", rf.currentTerm, rf.me, args.CandidateId, reply)

	// for candidate to update itself
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
	} else {
		reply.Term = rf.currentTerm
	}
	//1. reply false if term < currentTerm
	//2. if voteFor is null or candidateId, and candidate's log is at last as up-to-date as reveivers'log,grant vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.IsLogOlderOrEqual(args) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.ResetElectionTimeout()
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = args.Term
		rf.voteFor = -1 // 新任期我还没有给任何人投票
		if rf.IsLogOlderOrEqual(args) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.ResetElectionTimeout()
		}
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		currentTerm:     0,
		voteFor:         -1,
		log:             make([]logEntry, 1),
		commitIndex:     0,
		lastApplied:     0,
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		RaftStatus:      Follower,
		heartbeatsTimer: time.NewTimer(HeardBeatTimeout * time.Millisecond),
		electionTimer:   time.NewTimer(randomDuration()),
	}
	DPrintf(rf.me, "{Node %d} 完成了初始化", rf.me)
	rf.heartbeatsTimer.Stop()
	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
