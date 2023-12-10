package helper

import (
	"log"
)

var debugEnabled = true

// DPrintf 是一个自定义的调试日志打印函数，可以通过设置 debugEnabled 变量来控制是否打印日志。
func DPrintf(format string, args ...interface{}) {
	if debugEnabled {
		log.Printf(format, args...)
	}
}
