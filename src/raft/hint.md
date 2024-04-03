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
- 心跳间隔要求>100ms,所以可能要使用比150-300ms更久的选举超时
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
- 虽然状态切换时经常伴有任期号变化,任期号的变化取决于具体情况,所以放在ChangeStage外
- timer的开关与状态相关,所以放到ChangeStage内
- voteFor 的语义是当前任期为谁投了票,所以切换到新的任期时
    voteFor = -1
- 每当收到rpc,都要针对任期号做状态切换
- 确保len(log)获得的事日志的长度,在必要时需要对log取切片缩放
    - 确保append操作是追加到日志的尾部
- 函数A获取m锁的情况下调用B,B如果尝试获取m锁会造成死锁
    - 如果B放入新的goroutine中执行就不会死锁
    - 
- log 的初始长度为1,什么也不放.
	索引为index的日志,存放在log[index]
	[x] 空
	[x, 1, 2, 3] len = 4
	if len()
	LastIndex = len-1

# 思路梳理

## 1.日志定义
log := [{0, -1, _}, {1, term, command}, {2, term, command} ....,  {index, term, command}]
1. log[0] 为无效日志填充, 逻辑索引号与物理索引号统一了.索引为2的日志放在log[2]
2. log[0].Term == -1
3. 无log时(仅有log[0])
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
3. 最初空log时的心跳广播,nextIndex=1  prevLogIndex = 1-1 == 0,prevLogTerm == -1
4. prevLogIndex=0,prevLogTerm=-1,这能通过rf.matchLog的检查,reply.Success == true
5. 当上层追加日志后,prevLogIndex,PrevLogTerm不变.只有handleAppendEntriesReply中完成了nextIndex[peer]的更新后,prevLogIndex才会相应变化


## 使用心跳来完成日志复制的论述:
1. 当AppendEntriesReply.Success = False 时,只需nextIndex[peer] --
2. Leader收到上层的命令追加时,主动发起一次心跳
等待下一次心跳,这样的缺点是日志复制失败后,每一次失败都要等待一个心跳时延
考虑到失败不常发生,这样也可以接受.
3. 或者在失败时启动一个goroutine,对这个Follower一直追加,直到成功

## 日志复制逻辑链
### Leader
1. BroadcastAppendEntries 对于每一个peer发送sendAppendEntries
2. handleAppendEntriesReply中处理Follower的回复
    1. 更新matchINdex
    2. 更新commitIndex,注意只能提交自己任期内的日志
    3. 更新lastApplied,并通知上层
    4. 如果失败,nextIndex[peer] --, 等待下一次心跳.todo,专门开启一个goroutine
### Follower
1. 在AppendEntries_2B中处理AppendEntries 
    1. 2A 逻辑不变,完成任期更新,状态切换,选举超时重置,voteFor更新
    2. matchLog检查是否可以将Entries追加到本地log中
    3. 完成日志复制 
    4. 更新commitIndex
    5. 应用日志


# ToDo
- [x] 心跳广播 
- [x] RequestVote的任期有错误 !!!
- [x] 开始选举函数 Raft.StartElection()
- [x] ChangeStage 开关 两个timer
- [x] 检查是否有函数嵌套时,内外两层函数获取同一把锁
- [ ] debug: 在非leader状态下可能有心跳超时
- [ ] 修改get|prev|last|logTerm|Index逻辑,并通过2A测试
- [x] 在logEntry中增加了Index字段
---------------------------------------------------
- [x] AppendEntriesArgs 接收
- [x] AppendEntriesReply 接收
- [ ] 在追加日志函数中,追加对自己nextindex的维护
- [ ] bug:修改getPrevLogIndex逻辑
- [ ] 去掉isHeartBeat 逻辑
- [ ] bug: genRequestVoteArgs.Entries 中产生的log太多了,每次都产生全部的
- [ ] review 对空entries的处理
- [ ] handleAppendEntriesReply 中处理过期问题


# 疑问

-  commitIndex,lastApplied 作为易失状态如何如何在故障后恢复
-  往rf.applyCh中写入数据会不会阻塞







