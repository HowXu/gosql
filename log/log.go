package log

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/HowXu/gosql/debug"
	"github.com/HowXu/gosql/err"
)

// 全局LogFile指针
var LogFile *os.File
var writer *bufio.Writer

func Init(file *os.File) {
	//赋值指针和Writer
	LogFile = file
	writer = bufio.NewWriter(file)
}

func STDLOG(info string, extra_info string) {
	//1等级才进行打印
	if debug.LogLevel == 1 {
		fmt.Printf("[INFO]%d:%d:%d %s attach %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info, extra_info)
	}
}

func STD_SM_Log(info string) {
	if debug.LogLevel == 1 {
		fmt.Printf("[INFO]%d:%d:%d %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info)
	}
}

func STDERR(info string, extra_info string) {
	fmt.Printf("[ERROR]%d:%d:%d %s attach %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info, extra_info)
}

func STD_SM_ERR(info string) {
	fmt.Printf("[ERROR]%d:%d:%d %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info)
}

func FileLog(info string, extra_info string) {
	//创建log相关文件在core.Init进行

	//文件IO Log随时可用
	var _, err = writer.WriteString(fmt.Sprintf("[INFO]%d:%d:%d %s attach %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info, extra_info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}

func File_SM_Log(info string) {
	//文件IO Log随时可用
	var _, err = writer.WriteString(fmt.Sprintf("[INFO]%d:%d:%d %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}

func FileErr(info string, extra_info string) {
	//创建log相关文件在core.Init进行
	var _, err = writer.WriteString(fmt.Sprintf("[Error]%d:%d:%d %s attach %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info, extra_info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}

func File_SM_Err(info string) {
	//文件IO Log随时可用
	var _, err = writer.WriteString(fmt.Sprintf("[Error]%d:%d:%d %s\n", time.Now().Hour(), time.Now().Minute(), time.Now().Second(), info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}

func ALL_ERR(info string) error {
	STD_SM_ERR(info)
	File_SM_Err(info)
	return &err.DatabaseError{
		Msg: info,
	}
}

func ALL_LOG(info string) error {
	STD_SM_Log(info)
	File_SM_Log(info)
	return &err.DatabaseError{
		Msg: info,
	}
}

func ALL_ATA_ERR(info string, extra string) error {
	STDERR(info, extra)
	FileErr(info, extra)
	return &err.DatabaseError{
		Msg: info + "extra with " + extra,
	}
}

func Runtime_Log(info string) {
	fmt.Printf("INFO: %s\n\n",info)
}

func Runtime_Warn(info string) {
	fmt.Printf("WARNING: %s\n\n",info)
}

func Runtime_log_err(e error) error{
	fmt.Printf("INFO: %s\n\n",e.Error())
	return e
}
