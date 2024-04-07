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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	applyCh         chan ApplyMsg       // key-value层通过这个收到ApplyMsg, 最终应用日志内容
	currentTerm     int                 // latest term server has seen(initialized to 0 on first boot, increase monotonically<单调地>)
	voteFor         int                 // 当前任期内给 candidateId 投了赞成，如果没有给任何候选人投赞成 则为空.根据定义,切换到新任期时置为-1
	log             []logEntry          // 每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）fix:1
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

/*
## 1.日志约定
log := [{0, -1, _}, {1, term, command}, {2, term, command} ....,  {index, term, command}]
1. log[0] 为无效日志填充, 逻辑索引号与物理索引号统一了.索引为2的日志放在log[2]
2. log[0].Term == -1
3. 无log时(仅有log[0])
*/

// 返回最后一条日志的索引,若没有日志返回0
func (rf *Raft) getLastLogIndex() int {
	// [x] len = 1  target:0
	// [x, 1] len = 2 target:1
	// [x, 1, 2] len = 3  target:2
	// [x, 1, 2, 3] len = 4 target:3
	return len(rf.log) - 1
}

// 返回最后一条日志的任期号,若没有日志返回-1
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) <= 1 {
		return -1
	} else {
		return rf.log[rf.getLastLogIndex()].Term
	}
}

/*
这是对prevLog的错误理解
// 获得倒数第二条日志的任期,若没有日志返回-1
func (rf *Raft) getPrevLogTerm() int {
	if len(rf.log) < 3 {
		return -1
	} else {
		return rf.log[rf.getPrevLogIndex()].Term
	}
}
*/

// 用于一致性检查
func (rf *Raft) matchLog(prevLogIndex int, prevLogTerm int) bool {
	if rf.getLastLogIndex() < prevLogIndex { // rf.log[prevLogIndex] 越界
		return false
	}
	return rf.log[prevLogIndex].Term == prevLogTerm
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

func (rf *Raft) genAppendEntriesArgs(peer int) AppendEntriesArgs {
	rf.mu.Lock()
	rf.mu.Unlock()
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	if rf.nextIndex[rf.me] == rf.nextIndex[peer] {
		// rf.nextIndex[rf.me] == len(log)
		// 防止else情况中下标越界
		args.Entries = []logEntry{}
	} else {
		args.Entries = rf.log[rf.nextIndex[peer]:]
	}
	args.LeaderCommit = rf.commitIndex
	return args
}

// 监听心跳超时计时器和选举超时计时器
func (rf *Raft) ticker() {
	// tips: CampaignForVotes,BroadcastHeartbeat函数都是在加锁的情况下执行,因此要尽快返回,避免性能下降
	// test 并不会停止goroutine
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf(rf.me, "{Node %v} 选举计时器超时 ", rf.me)
			if rf.RaftStatus == Leader {
				panic("Leader状态 选举计时器超时")
			}
			// 选举流程 1. 将自身切换为Candidate 状态 2. 任期号+1 3. 请求选票 4. 重置选举超时计时器
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.CampaignForVotes() // 函数立即返回,内部耗时过程放入新goroutines
			rf.ElectionTimerReset()
			rf.mu.Unlock()
		case <-rf.heartbeatsTimer.C:
			rf.mu.Lock()
			if rf.RaftStatus != Leader {
				panic("非Leader状态 心跳计时器超时")
			}
			rf.BroadcastAppendEntries()
			rf.HeardBeatReset()
			rf.mu.Unlock()
		}
	}
}

// 广播AppendEntries
func (rf *Raft) BroadcastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := rf.genAppendEntriesArgs(peer)
			reply := AppendEntriesReply{}
			DPrintInfo(rf)
			DPrintf(rf.me, log2str(-1, rf.log))
			DPrintf(rf.me, "lastApplied: %v", rf.lastApplied)
			DPrintf(rf.me, "Term: %v | {%v}->{%v} ApArgs: %v", rf.currentTerm, rf.me, peer, args2str(&args))
			////DPrintf(rf.me, "[try] Term: %v | {Node %v} -> {Node %v} AppendEntriesArgs", rf.currentTerm, rf.me, peer)
			if !rf.sendAppendEntries(peer, &args, &reply) {
				DPrintf(rf.me, "[Error] Term: %v | {%v}->{%v} ApArgs: %v", rf.currentTerm, rf.me, peer, args2str(&args))
				return
			}
			//DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} AppendEntriesArgs: %v", rf.currentTerm, rf.me, peer, args2str(&args))
			DPrintf(rf.me, "Term: %v | {%v}<-{%v} ApReply: %v", rf.currentTerm, rf.me, peer, reply)
			rf.handleAppendEntriesReply(peer, args, reply)
		}(peer)
	}
}

func (rf *Raft) handleAppendEntriesReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} AppendEntriesReply: %v", rf.currentTerm, rf.me, peer, reply)
	///defer rf.persist()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.ChangeState(Follower)
		rf.voteFor = -1
		return
	}
	if !reply.Success {
		// todo 快速恢复逻辑
		rf.nextIndex[peer]--
		return

	}
	// 考虑重复报文情况
	/*
			F: [x, 1, 2, 3]  _
		             pI->^   ^<-nx
			L: [x, 1, 2, 3, 4, 5, 6, 7]
			entries: [4, 5, 6, 7]
			len(entries) = 4
			rf.matchIndex = 7 = pI + len(entries) = 3 + 4 = 7
			rf.nextIndex = 8 = nx + len(entries) = 4 + 4 = 8
	*/
	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
	// 检查
	if rf.nextIndex[peer] > rf.getLastLogIndex()+1 {
		panic("Follower nextIndex 超过 Leader")
	}
	if rf.getLastLogIndex() != rf.matchIndex[rf.me] {
		panic("Leader: matchIndex[me] != getLastLogIndex")
	}
	if rf.getLastLogIndex()+1 != rf.nextIndex[rf.me] {
		panic("Leader: nextIndex[me] != getLastLogIndex + 1 ")
	}
	rf.LeaderUpdateCommitIndex()
	rf.ApplyLog()
}

// Leader 根据日志复制情况,更新commitIndex
func (rf *Raft) LeaderUpdateCommitIndex() {
	// 哪些日志已复制到多数节点
	t := make([]int, len(rf.matchIndex))
	copy(t, rf.matchIndex)
	sort.Ints(t)
	maxIndex := t[len(rf.peers)/2]               // 5/2=2, 0,1,2 [2]是第三个
	if rf.log[maxIndex].Term == rf.currentTerm { // 通过计数的方式只能提交自己任期内的日志
		rf.commitIndex = maxIndex
	}
	if rf.commitIndex > rf.getLastLogIndex() {
		panic("Leader: commitIndex > getLastLogIndex")
	}

}

// 提交 lastApplied < log.index <= commitIndex
func (rf *Raft) ApplyLog() {
	if rf.commitIndex > rf.getLastLogIndex() {
		panic("Leader: commitIndex > getLastLogIndex")
	}
	for rf.lastApplied < rf.commitIndex {
		//fmt.Println(rf)
		DPrintInfo(rf)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied+1].Command, //fix:1
			CommandIndex: rf.lastApplied + 1,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied++
	}
}

// 请求其他Server 为自己投票
// 🔐 在持有锁的状态下被调用
func (rf *Raft) CampaignForVotes() {
	args := rf.genRequestVoteArgs() // 不要放到下面goroutine中产生
	rf.voteFor = rf.me
	///rf.persist()
	currentVoteCount := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// P:
		// 1. 在对raft结构体加锁的情况下下执行CampaignForVotes
		// 2. 锁在CampaignForVotes束之后才释放
		// 3. 发送RequestVoteArgs不需要获取锁(锁用来保护Raft数据结构)
		// 4. 在下面的goroutine中,是在收到RequestVoteReply后尝试获取锁
		// P->Q:
		//   1. RequestVoteArgs的发出是紧凑的
		//   2. 在CampaignForVotes结束后,goroutines才陆续看到RequestVoteReply的内容

		// question:
		// 虽然RequestVoteArgs的发出是紧凑的,但是并不是原子的,可能在发送的过程中变成了Follower或Leader
		// 那在状态改变了的情况下,是否还有必要继续发送?
		// answer:
		// args的生成是最初的时刻,goroutines发出的RequestVoteArgs是一样的.发出的早晚受go调度影响,这带来
		// 的延时远小于一个网络RTT,将这块时延并入RTT,可以等价于所有的RequestVoteArgs是同时发出的

		go func(peer int) {
			var reply RequestVoteReply
			// 因为网络或者宕机,不一定发送成功
			DPrintf(rf.me, "[try]: Term: %v | {Node %v} -> {Node %v} RequestVoteArgs: %v", rf.currentTerm, rf.me, peer, args)
			if !rf.sendRequestVote(peer, &args, &reply) {
				DPrintf(rf.me, "[error]: Term: %v | {Node %v} 未收到 {Node %v} 的RequestVoteReply 对应 args: %v", rf.currentTerm, rf.me, peer, args)
				return
			}
			DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} RequestVoteArgs: %v", rf.currentTerm, rf.me, peer, args)
			DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} RequestVoteReply: %v", rf.currentTerm, rf.me, peer, reply)
			// 收到reply才加锁改变raft状态
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 发出RequestVoteArgs,到收到RequestVoteReply之间状态可能已经发生了改变
			// 根据图4的状态转换
			// 1. candidate还在args.Term任期收集选票
			// 2. 搜集超过半数选票,已成为args.Term任期的Leader
			// 3. arg.Term任期内未能收集超半数选票,选举计时器超时,已进入新一轮选举
			// 4. 收到Leader的AppendEntriesArgs,已转换成Follower
			// 5. 看到更高的任期号T,set currentTerm=T,已转变为Follower
			// State
			// case 1: candidate && currentTerm == args.Term
			// case 2: Leader && currentTerm == args.Term
			// case 3: Candidate && currentTerm > args.Term
			// case 4: Follower && currentTerm >= args.Term, 新Leader可能是同期的选民,也可能是因为网络分区,才收到一个领先分区的Leader的AppendEntriesArgs
			// case 5: Follower && currentTerm > args.Term

			// case2 - case5 意味着当前节点在args.Term任期选举结束,所以只需要处理case1
			if !(rf.currentTerm == args.Term && rf.RaftStatus == Candidate) {
				return
			}
			if reply.VoteGranted {
				currentVoteCount += 1
				if currentVoteCount > len(rf.peers)/2 {
					DPrintf(rf.me, "Term: %v | {Node %v} 收到了半数选票当选 Leader", rf.currentTerm, rf.me)
					rf.ChangeState(Leader)
				}
				// 是否投票两个因素: 1.任期 2.LastLog
				// 如果是因为任期原因,else包含两个原因,所以这里用else if
			} else if reply.Term > args.Term {
				// 只会发生一次,这段代码只有在Candidate才能到达,执行后状态转变为Follower
				DPrintf(rf.me, "[Warning]: Term : %v | {Node %v} <- {Node %v}  RequestVoteReply: %v 中包含更高任期号:%v", rf.currentTerm, rf.me, peer, reply, reply.Term)
				rf.ChangeState(Follower)
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				///rf.persist()
			}
		}(peer)
	}

}

// 🔐
func (rf *Raft) ChangeState(Rs RaftStatus) {
	switch Rs {
	case Leader:
		// 图2 Volatile state on leader,下面蓝字 Reinitialized after election
		//initialized to leader last log index + 1
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
		//initialized to 0
		for i := 0; i < len(rf.matchIndex); i++ {
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
		// leader 关闭选举超时计时器
		rf.ElectionTimerStopAndClean()
		// leader 开启心跳超时计时器
		rf.HeardBeatReset()
		rf.RaftStatus = Leader
	case Candidate:
		// 打开选举超时计时器
		rf.ElectionTimerReset()
		// 关闭心跳超时计时器
		rf.HeardBeatStopAndClean()
		rf.RaftStatus = Candidate
	case Follower:
		// 打开选举超时计时器
		rf.ElectionTimerReset()
		// 关闭心跳超时计时器
		rf.HeardBeatStopAndClean()
		rf.RaftStatus = Follower
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.RaftStatus == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
    rf.persister.SaveRaftState(data)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&log) != nil {
		panic("readPersist: decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
	}
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
	PrevLogIndex int        // 用来确保Follower日志与Leader同步,值为rf.nextIndex[peer]-1
	PrevLogTerm  int        // 用来确保Follower日志与Leader同步,值为log[prevLogIndex].Term
	Entries      []logEntry //需要flower被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导人的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  //当前任期号,for leader to update itself
	Success bool // flower在preveLogIndex位置的日志的任期是prevlogTerm, 则为true
	/*
		- XTerm：这个是Follower中与Leader冲突的Log对应的任期号。
			在之前（7.1）有介绍Leader会在prevLogTerm中带上本地Log记录中，
			前一条Log的任期号。如果Follower在对应位置的任期号不匹配，它
			会拒绝Leader的AppendEntries消息，并将自己的任期号放在XTerm中。
			如果Follower在对应位置没有Log，那么这里会返回 -1。
		- XIndex：这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。
		- XLen：如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数。
	*/
}

// args 指向的日志至少和rf一样新
// 选举限制
func (rf *Raft) IsLogOlderOrEqual(args *RequestVoteArgs) bool {
	raftLastLogTerm := rf.getLastLogTerm()
	raftLastLogIndex := rf.getLastLogIndex()
	// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新。
	// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
	// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	if args.LastLogTerm > raftLastLogTerm || (args.LastLogTerm == raftLastLogTerm && args.LastLogIndex >= raftLastLogIndex) {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries_2A(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	///defer rf.persist()
	///DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} AppendEntriesArgs: %v", rf.currentTerm, rf.me, args.LeaderId, args)
	DPrintf(rf.me, "Before:log: %v", log2str(-1, rf.log))
	DPrintf(rf.me, "Term: %v | {%v}<-{%v} ApArgs %v", rf.currentTerm, rf.me, args.LeaderId, args2str(args))
	defer DPrintf(rf.me, "Term: %v | {%v}->{%v} ApReply: %v", rf.currentTerm, rf.me, args.LeaderId, reply)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term == rf.currentTerm {
		// 同一任期不可能有两个Leader,当前节点Follower 或者 Candidate
		reply.Term = rf.currentTerm
		if rf.RaftStatus != Follower {
			rf.ChangeState(Follower)
		}
		rf.ElectionTimerReset()
	} else if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.voteFor = -1
		if rf.RaftStatus != Follower {
			rf.ChangeState(Follower)
		}
		rf.ElectionTimerReset()
	}
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		// todo 为快速恢复要提供更多信息
		return
	}
	// success = true, if Follower contained entry matching prevLogIndex and prevLogTerm
	reply.Success = true

	// 可以追加日志了
	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	// 这里的 if 至关重要。如果追随者拥有领导者发送的所有条目，则追随者不得截断其日志。
	// 领导者发送的条目后面的任何元素都必须保留。
	// 这是因为我们可能会从领导者那里收到过时的 AppendEntries RPC，
	// 并且截断日志意味着“收回”我们可能已经告诉领导者我们在日志中拥有的条目。
	// 只有AppendEntriesArgs里的entries和Follower的log冲突时,才能将Follower的log截断

	// 综合考虑go的语法与图2要求, 分两种情况讨论
	// case1 : 最后一条日志无法通过下标放入log
	//      eg: log: [x, 1, 2, 3, 4, 5] prevLogIndex = 3  entries = [4, 5, 6, 7, 8, 9]  这些数字都是index,省略了其他信息
	//                         ^ <- prevLogIndex                                    ^ <- entriesLastIndex
	//      直接将prevLogIndex后面的截断,然后追加entries
	// case2 : entries的最后一条日志能通过下标放入log
	//      eg: log[x, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, ....100]  entries[(4, term, command), (5 ...), (6...)]
	//		tips: 索引为i的entry, 要放到索引为i的位置, entries最后一条索引9, 放入log[9], 要求长度至少10
	//      1. 待插入的entries的下标都是合法下标
	//      2. 遍历看是否需要截断
	//      3. 如果需要截断,直接将prevLogIndex后面的截断,然后追加entries
	///DPrintf(rf.me, "entries 不为空")

	entriesLastIndex := args.PrevLogIndex + len(args.Entries)
	if len(rf.log) <= entriesLastIndex {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) //tips 前闭后开
	} else {
		truncFlag := false
		i := args.PrevLogIndex + 1
		for _, entry := range args.Entries {
			if entry.Term != rf.log[i].Term { // index相同,term不相同
				truncFlag = true
				break
			}
			rf.log[i] = entry
			i += 1
		}
		if truncFlag {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		}
	}
	// 提交
	// The min in the final step (#5) of AppendEntries is necessary,
	// and it needs to be computed with the index of the last new entry.
	// It is not sufficient to simply have the function that applies
	// things from your log between lastApplied and commitIndex stop
	// when it reaches the end of your log. This is because you may
	// have entries in your log that differ from the leader’s log after
	// the entries that the leader sent you (which all match the ones in your log).
	// Because #3 dictates that you only truncate your log if you have conflicting entries,
	// those won’t be removed, and if leaderCommit is beyond the entries the leader sent you,
	// you may apply incorrect entries.
	// case2 : entries的最后一条日志能通过下标放入log
	//      eg: log[x, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, ....100]  entries[(4, term, command), (5 ...), (6...)]
	//		tips: 索引为i的entry, 要放到索引为i的位置, entries最后一条索引9, 放入log[9], 要求长度至少10
	// 如果entries所有的entry都不冲突,这种情况下,是不会截断 7-100的.如果此时LeaderCommit >= 7,可能提交一些不正确的日志
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.PrevLogIndex+len(args.Entries) < args.LeaderCommit {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		}
	}
	for rf.lastApplied < rf.commitIndex {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied+1].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.lastApplied++
		rf.applyCh <- applyMsg
	}
	DPrintf(rf.me, "Before:log: %v", log2str(-1, rf.log))
	DPrintf(rf.me, "lastApplied: %v", rf.lastApplied)
}

// 收到RequestVoteArgs的处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(rf.me, "Term: %v | {Node %v} <- {Node %v} RequestVoteArgs: %v", rf.currentTerm, rf.me, args.CandidateId, args)
	defer DPrintf(rf.me, "Term: %v | {Node %v} -> {Node %v} RequestVotereply: %v", rf.currentTerm, rf.me, args.CandidateId, reply)
	///defer rf.persist()

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
		return
	} else if args.Term == rf.currentTerm {
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.IsLogOlderOrEqual(args) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.ElectionTimerReset()
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm = args.Term
		rf.voteFor = -1
		if rf.IsLogOlderOrEqual(args) {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.ElectionTimerReset()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	index := -1
	term := -1
	isLeader := true
	/*
		if rf.killed() {
			return index, term, false
		}
	*/
	if rf.RaftStatus != Leader {
		return index, term, false
	}

	// 将command封装进entry
	//currentLogEntry := logEntry{Term: rf.currentTerm, Command: command, Index: rf.getLastLogIndex() + 1}
	currentLogEntry := logEntry{Term: rf.currentTerm, Command: command}
	///DPrintf(rf.me, "Term: %v | start->Entry %v", rf.currentTerm, currentLogEntry)
	DPrintf(rf.me, "Term: %v | start->Entry %v", rf.currentTerm, log2str(rf.getLastLogIndex(), []logEntry{currentLogEntry}))
	rf.log = append(rf.log, currentLogEntry)
	rf.nextIndex[rf.me] = rf.getLastLogIndex() + 1
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.BroadcastAppendEntries()

	term = rf.currentTerm
	index = rf.getLastLogIndex()
	isLeader = true
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
		dead:            0,
		applyCh:         applyCh,
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
	//rf.heartbeatsTimer.Stop()
	rf.HeardBeatStopAndClean()
	// 日志定义
	rf.log[0].Term = -1

	go rf.ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) HeardBeatStopAndClean() {
	// 此时定时器情况
	// 1. 管道中没有值,正在计时
	// 2. 管道中没有值,已经停止
	// 3. 管道中有值,正在计时
	// 4. 管道中有值,已经停止
	// stop 的返回值只是告诉你,是否成功阻止了一个尚未出发的计时器.
	if !rf.heartbeatsTimer.Stop() {
		//如果定时器已经超时,清空通道
		select {
		case <-rf.heartbeatsTimer.C:
			// 清空通道
		default:
		}
	}
	// 此时对于 1 2 4 情况管道中已经没有值了.但是情况3 管道中还有值
	// 所以要无阻塞的再清理一次
	select {
	case <-rf.heartbeatsTimer.C:
		// 清空通道
	default:
	}
}

func (rf *Raft) HeardBeatReset() {
	rf.HeardBeatStopAndClean()
	rf.heartbeatsTimer.Reset(HeardBeatTimeout * time.Millisecond)
}

func (rf *Raft) ElectionTimerStopAndClean() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	select {
	case <-rf.heartbeatsTimer.C:
		// 清空通道
	default:
	}
}

func (rf *Raft) ElectionTimerReset() {
	rf.ElectionTimerStopAndClean()
	rf.electionTimer.Reset(randomDuration())
}
