package raft

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
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
	if Debug <= 0 {
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

func DPrintInfo(rf *Raft) {
	format := `
   RaftStatus: %v
   currentTerm: %v
   log: %v
   commitIndex: %v
   lastApplied: %v
   nextIndex[]: %v
   matchIndex[]: %v \n
   `
	fmt.Printf(format, rf.RaftStatus, rf.currentTerm, log2str(-1, rf.log), rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	return
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
	s := "T: %v LId: %v PI: %v PT: %v Entry: %v LC: %v"
	s = fmt.Sprintf(s, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, log2str(args.PrevLogIndex, args.Entries), args.LeaderCommit)
	return s
}
