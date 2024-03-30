package raft

import (
	"fmt"
	"strings"
)

// Debugging
const Debug = 2

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

var colors = []string{colorRed, colorGreen, colorYellow, colorBlue, colorPurple, colorCyan, colorWhite}

/*
func DPrintf(peer int, format string, a ...interface{}) (n int, err error) {
	if Debug <= 0 {
		return
	}
	temp := ""
	for i := 0; i < peer; i++ {
		temp += "									"
	}
	color := colors[peer%len(colors)] // 循环使用颜色
	format = fmt.Sprintf("%s%s\n", color, temp+format) + colorReset
	fmt.Printf(format, a...)
	return
}

*/
func DPrintf(peer int, format string, a ...interface{}) (n int, err error) {
    if Debug <= 0 {
        return
    }

    // 设置每个peer的列宽度
    //const columnWidth = 55
    const columnWidth = 0

    // 根据peer计算前导空格的数量
    padding := strings.Repeat(" ", peer*columnWidth)

    // 循环使用颜色
    color := colors[peer%len(colors)]

    // 构建带有颜色、前导空格和格式化字符串的输出格式
    format = fmt.Sprintf("%s%s%s", color, padding, format) + colorReset + "\n"

    // 执行格式化打印
    fmt.Printf(format, a...)
    return
}

