package log

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/HowXu/gosql/debug"
)

//全局LogFile指针
var LogFile *os.File
var writer *bufio.Writer

func Init(file *os.File ){
	//赋值指针和Writer
	LogFile = file
	writer = bufio.NewWriter(file)
}

func STDLOG(info string,extra_info string) {
	//1等级才进行打印
	if debug.LogLevel == 1 {
		fmt.Printf("[INFO]%d:%d:%d %s attach %s\n",time.Now().Hour(),time.Now().Minute(),time.Now().Second(),info,extra_info)
	}
}

func STD_SM_Log(info string){
	if debug.LogLevel == 1 {
		fmt.Printf("[INFO]%d:%d:%d %s\n",time.Now().Hour(),time.Now().Minute(),time.Now().Second(),info)
	}
}

func FileLog(info string,extra_info string) {
	//创建log相关文件在core.Init进行

	//文件IO Log随时可用
	var _,err = writer.WriteString(fmt.Sprintf("[INFO]%d:%d:%d %s attach %s",time.Now().Hour(),time.Now().Minute(),time.Now().Second(),info,extra_info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}

func File_SM_Log(info string) {
	//文件IO Log随时可用
	var _,err = writer.WriteString(fmt.Sprintf("[INFO]%d:%d:%d %s",time.Now().Hour(),time.Now().Minute(),time.Now().Second(),info))
	if err != nil {
		STD_SM_Log("FileLog 文件IO 出错")
	}
	//刷新缓冲区
	writer.Flush()
}