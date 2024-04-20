# TodoList

- [x] Add the Figure 1 state for leader election to the Raft struct in raft.go.
- [x] define a struct to hold information about each log entry.
- [x] Fill in the RequestVoteArgs and RequestVoteReply structs.

- [ ] Modify Make() to create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.This way a peer will learn who is the leader, if there is already a leader, or become the leader itself.
- [ ] Implement the RequestVote() RPC handler so that servers will vote for one another.

To implement heartbeats

- [ ] define an AppendEntries RPC struct (though you may not need all the arguments yet)
- [ ] and have the leader send them out periodically.
- [ ] Write an AppendEntries RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.

- Make sure the election timeouts in different peers don't always fire at the same time,
  or else all peers will vote only for themselves and no one will become the leader.

# Hint

- 心跳间隔>100ms
- 心跳间隔要求>100ms,所以可能要使用比 150-300ms 更久的选举超时
- 您可能会发现 Go 的 rand 很有用。
- 您需要编写定期或在延迟后执行操作的代码。最简单的方法是创建一个带有调用 time.Sleep() 的循环的 goroutine。不要使用 Go 的 time.Timer 或 time.Ticker ，它们很难正确使用。
- Read this advice about locking and structure.
  阅读有关锁定和结构的建议。
- [x] Don't forget to implement GetState().
- [x] The tester calls your Raft's rf.Kill() when it is permanently shutting down an instance. You can check whether Kill() has been called using rf.killed(). You may want to do this in all loops, to avoid having dead Raft instances print confusing messages.
- 在接受/回复消息打印消息,将信息收集到一个文件中
  ```shell
  go test -run 2A > out
  ```
- 使用 go test -race 检查您的代码，并修复它报告的任何竞争。

# 随手记

- 虽然状态切换时经常伴有任期号变化,任期号的变化取决于具体情况,所以放在 ChangeStage 外
- timer 的开关与状态相关,所以放到 ChangeStage 内
- voteFor 的语义是当前任期为谁投了票,所以切换到新的任期时
  voteFor = -1
- 每当收到 rpc,都要针对任期号做状态切换
- 确保 len(log)获得的事日志的长度,在必要时需要对 log 取切片缩放
  - 确保 append 操作是追加到日志的尾部
- 函数 A 获取 m 锁的情况下调用 B,B 如果尝试获取 m 锁会造成死锁
  - 如果 B 放入新的 goroutine 中执行就不会死锁
  -
- log 的初始长度为 1,什么也不放.
  索引为 index 的日志,存放在 log[index]
  [x] 空
  [x, 1, 2, 3] len = 4
  if len()
  LastIndex = len-1
- 那些只调用一次的函数,将锁放在函数前后,比放在函数内部可读性更好
- test 并不会停止goroutine,所以要通过rf.killed来停止对外提供服务
- 如果不做快速恢复,lab2 TestBackup2B 有些情况下无法通过测试


# 关于进行持久化的时间节点
## 需要持久化的状态

1. currentTerm
2. voteFor
3. log
4. 这三个变量一旦发生变化就一定要在被其他协程感知到之前（释放锁之前，发送 rpc 之前）持久化，这样才能保证原子性。

## 1.选举计时器超时,发送 RequestVoteArgs 之前

1. 增加任期号 --ticker
2. 为自己投票 --CampaignForVotes
3. 对 log 没有改变

所以在这 1,2 操作之后 rf.persist()

## 2.收到 RequestVoteArgs 之后

1.  当 reply.Term > args.Term 时, currentTerm 增加, voteFor = -1
2.  log 不变

## 3.发送 AppendEntriesArgs 之前

genRequestVoteArgs 需要持久化的状态没有改变

## 4.收到 AppendEntriesArgs 之后

需要持久化的状态都有可能发生变化

## 5.收到 AppendEntriesReply 之后

1. 当任期号更高时,currentTerm 和 voteFor 需要更改
2. log 不会修改

## 6.start 向leader追加日志


- Leader 发送 AppendEntriesArgs 之前
  1.  持久化状态没有改变
- 收到 AppendEntriesArgs
  1.  任期可能增加,voteFor=-1
  2.  log 可能增加
- 收到 AppendEntriesReply
-
- 发送 AppendEntriesArgs
  1.  持久化状态没有改变

# 思路梳理

## 1.日志定义

log := [{0, -1, _}, {1, term, command}, {2, term, command} ...., {index, term, command}]

1. log[0] 为无效日志填充, 逻辑索引号与物理索引号统一了.索引为 2 的日志放在 log[2].
2. getLastLogIndex: 最大合法下标, 方便越界检查
3. log[0].Term == -1
4. 无 log 时(仅有 log[0])

### 1.1RequestVote RPC

1. RequestVoteArgs.LastLogIndex = 0
2. RequestVoteArgs.LastLogTerm = -1
   以上两个字段用于日志新旧比较

```go
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
```

### 1.2 AppendEntries RPC

1. AppendEntriesArgs.prevLogIndex = rf.nextIndex[peer] - 1
2. AppendEntriesArgs.PrevLogTerm = rf.log[prevLogIndex].Term
3. 最初空 log 时的心跳广播,nextIndex=1 prevLogIndex = 1-1 == 0,prevLogTerm == -1
4. prevLogIndex=0,prevLogTerm=-1,这能通过 rf.matchLog 的检查,reply.Success == true
5. 当上层追加日志后,prevLogIndex,PrevLogTerm 不变.只有 handleAppendEntriesReply 中完成了 nextIndex[peer]的更新后,prevLogIndex 才会相应变化

## 使用心跳来完成日志复制的论述:

1. 当 AppendEntriesReply.Success = False 时,只需 nextIndex[peer] --
2. Leader 收到上层的命令追加时,主动发起一次心跳
   等待下一次心跳,这样的缺点是日志复制失败后,每一次失败都要等待一个心跳时延
   考虑到失败不常发生,这样也可以接受.
3. 或者在失败时启动一个 goroutine,对这个 Follower 一直追加,直到成功

## 日志复制逻辑链

### Leader

1. BroadcastAppendEntries 对于每一个 peer 发送 sendAppendEntries
2. handleAppendEntriesReply 中处理 Follower 的回复
   1. 更新 matchINdex
   2. 更新 commitIndex,注意只能提交自己任期内的日志
   3. 更新 lastApplied,并通知上层
   4. 如果失败,nextIndex[peer] --, 等待下一次心跳.todo,专门开启一个 goroutine

### Follower

1. 在 AppendEntries_2B 中处理 AppendEntries
   1. 2A 逻辑不变,完成任期更新,状态切换,选举超时重置,voteFor 更新
   2. matchLog 检查是否可以将 Entries 追加到本地 log 中
   3. 完成日志复制
   4. 更新 commitIndex
   5. 应用日志

# ToDo

- [x] 心跳广播
- [x] RequestVote 的任期有错误 !!!
- [x] 开始选举函数 Raft.StartElection()
- [x] ChangeStage 开关 两个 timer
- [x] 检查是否有函数嵌套时,内外两层函数获取同一把锁
- [x] debug: 在非 leader 状态下可能有心跳超时
- [x] 修改 get|prev|last|logTerm|Index 逻辑,并通过 2A 测试
- [x] 在 logEntry 中增加了 Index 字段

---

- [x] AppendEntriesArgs 接收
- [x] AppendEntriesReply 接收
- [x] 在追加日志函数中,追加对自己 nextindex 的维护
- [x] bug:修改 getPrevLogIndex 逻辑
- [x] 去掉 isHeartBeat 逻辑
- [x] bug: genRequestVoteArgs.Entries 中产生的 log 太多了,每次都产生全部的
- [x] review 对空 entries 的处理
- [x] handleAppendEntriesReply 中处理过期问题
- [x] 全局审查报文重复问题
- [x] 全局 review,修改注释
- [x] logEntry 添加 Index 的必要性,论文中没有此字段,但是演示动画中有.
- [x] logEntry 的全局修改索引 fix:1
- [x] RPC接收端可能收到重复的报文,review注意
- [x] 当某一个节点在一个独立的网络分区,他会不断的选举超时,然后任期号++. 
	但是目前对待旧任期的rpc请求都是忽略.当这个节点重新练会网络如何融入新的集体
	他会在AppendEntriesReply报文中告诉Leader更新的Term

- [x] bug: 当不是Leader后停止发送AppendEntriesArgs报文
- [ ] perf: 如果在调用Start时不立即BroadcastAppendEntries,这会影响写入时延,如果立即写入,当大量写入发生时,会有大量的重复log

# 疑问

- commitIndex,lastApplied 作为易失状态如何如何在故障后恢复
- commitIndex 是一个集体状态,可以通过和 Leader 的交流来获得
- 往 rf.applyCh 中写入数据会不会阻塞
- 在 Raft 协议的实现中，`lastApplied`用于记录当前节点已经应用到其状态机的最新日志条目索引。
  这个值仅在本地节点维护，而且 Raft 论文并未要求将其持久化。
  假设在某个节点发生崩溃之前，`lastApplied`的值为 7。如果该节点崩溃并重启，
  `lastApplied`可能会重置为 0（或初始值）。这种情况下，Raft 是否会再次告诉上层
  比如建立在 Raft 之上的键值存储系统），所有直到索引 7 为止的日志条目都可以被应用？
  如果是这样，是否就存在重复应用已经应用过的日志条目的风险？如果存在这样的风险，
  通常有哪些策略来避免或处理这种重复应用的问题？



# logTopic
- dClient (CLNT) - 与客户端相关的操作或交互，如客户端请求的接收和处理。
- dCommit (CMIT) - 提交操作，标志着一个或多个Raft日志条目已被安全存储且可应用到状态机。
- dDrop (DROP) - 丢弃消息或数据，可能是因为消息无效或在处理过程中遇到错误。
- dError (ERRO) - 错误事件，通常是系统遇到无法正常处理的异常情况。
- dInfo (INFO) - 提供一般性信息，用于记录系统状态或非关键事件。
- dLeader (LEAD) - 与领导者选举或领导者活动相关的事件，如领导者的更换。
- dLog (LOG1), dLog2 (LOG2) - 日志相关事件，可能涉及日志的创建、修改或复制。在Raft中，这通常指日志复制过程。
- dPersist (PERS) - 持久化相关事件，如日志条目或当前状态的持久化存储。
- dSnap (SNAP) - 快照操作，通常与状态机的快照创建和恢复相关。
- dTerm (TERM) - 与任期相关的事件，如任期的开始、结束或任期内发生的重要动作。
- dTest (TEST) - 测试相关的日志记录，可能用于调试或验证系统功能。
- dTimer (TIMR) - 定时器事件，可能与选举超时、心跳超时等计时任务相关。
- dTrace (TRCE) - 跟踪信息，用于详细记录系统的内部操作和决策过程。
- dVote (VOTE) - 投票事件，涉及Raft选举过程中的投票行为。
- dWarn (WARN) - 警告事件，用于提示可能的问题，但不一定影响系统的正常运行。
