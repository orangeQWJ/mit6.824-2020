package raft

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Debugging
const Debug = 0

const (
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	colorReset  = "\033[0m"
)

/*
const (
	colorRed    = "# "
	colorGreen  = "## "
	colorYellow = "### "
	colorBlue   = "#### "
	colorPurple = "##### "
	colorCyan   = "###### "
	colorWhite  = "\033[37m"
	colorReset  = "\033[0m"
)
*/

var colors = []string{colorRed, colorGreen, colorYellow, colorBlue, colorPurple, colorCyan, colorWhite}

func DPrintf(peer int, format string, a ...interface{}) {
	if Debug != 3 {
		return
	}

	// 设置每个peer的列宽度
	//const columnWidth = 55
	const columnWidth = 50

	// 根据peer计算前导空格的数量
	padding := strings.Repeat(" ", peer*columnWidth)

	// 循环使用颜色
	color := colors[peer%len(colors)]

	// 构建带有颜色、前导空格和格式化字符串的输出格式
	// format = fmt.Sprintf("%s%s%s", color, padding, format) + colorReset + "\n"
	format = fmt.Sprintf("%s%s%s", color, padding, format) + "\n"

	// 执行格式化打印
	fmt.Printf(format, a...)
	return
}

func raftInfo2str(rf *Raft) string {
	format := `
   RaftStatus: %v
   currentTerm: %v
   log: %v
   commitIndex: %v
   lastApplied: %v
   nextIndex[]: %v
   matchIndex[]: %v \n
   `
	s := fmt.Sprintf(format, rf.RaftStatus, rf.currentTerm, log2str(-1, rf.log), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	return s
}
func computeHash(input string) string {
	h := sha256.New()
	h.Write([]byte(input))
	hashed := h.Sum(nil)
	hashHex := hex.EncodeToString(hashed)
	return hashHex[:4] // 截取前 8 个字符
}

func log2str(prevLogIndex int, log []logEntry) string {
	s := ""
	for i, l := range log {
		cHash := computeHash(fmt.Sprintf("%v", l.Command))
		s += fmt.Sprintf("{%v,%v,%v} ", i+1+prevLogIndex, l.Term, cHash)
	}
	s = "[ " + s + " ]"
	return s
}

func args2str(args *AppendEntriesArgs) string {
	//s :="args:\nTerm: %v\n LeaderId: %v\n PrevLogIndex: %v\n PrevLogTerm: %v \nlen(Entries): %v\n LeaderCommit: %v\n"
	//s := "Term: %v LeaderId: %v PrevLogIndex: %v PrevLogTerm: %v Entries: %v LeaderCommit: %v"
	s := "T:%v LId:%v PI:%v PT:%v Entry:%v LC:%v"
	//s = fmt.Sprintf(s, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, log2str(args.PrevLogIndex, args.Entries), args.LeaderCommit)
	s = fmt.Sprintf(s, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	return s
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugP(topic logTopic, peer int, format string, a ...interface{}) {
	if Debug == 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v %v ", time, string(topic), peer)
		// time topic peer
		format = prefix + format
		log.Printf(format, a...)
	}
}

/*
dClient (CLNT) - 与客户端相关的操作或交互，如客户端请求的接收和处理。
dCommit (CMIT) - 提交操作，标志着一个或多个Raft日志条目已被安全存储且可应用到状态机。
dDrop (DROP) - 丢弃消息或数据，可能是因为消息无效或在处理过程中遇到错误。
dError (ERRO) - 错误事件，通常是系统遇到无法正常处理的异常情况。
dInfo (INFO) - 提供一般性信息，用于记录系统状态或非关键事件。
dLeader (LEAD) - 与领导者选举或领导者活动相关的事件，如领导者的更换。
dLog (LOG1), dLog2 (LOG2) - 日志相关事件，可能涉及日志的创建、修改或复制。在Raft中，这通常指日志复制过程。
dPersist (PERS) - 持久化相关事件，如日志条目或当前状态的持久化存储。
dSnap (SNAP) - 快照操作，通常与状态机的快照创建和恢复相关。
dTerm (TERM) - 与任期相关的事件，如任期的开始、结束或任期内发生的重要动作。
dTest (TEST) - 测试相关的日志记录，可能用于调试或验证系统功能。
dTimer (TIMR) - 定时器事件，可能与选举超时、心跳超时等计时任务相关。
dTrace (TRCE) - 跟踪信息，用于详细记录系统的内部操作和决策过程。
dVote (VOTE) - 投票事件，涉及Raft选举过程中的投票行为。
dWarn (WARN) - 警告事件，用于提示可能的问题，但不一定影响系统的正常运行。
*/
