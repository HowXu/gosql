package core

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/HowXu/gosql/log"
)

//解析数据库文件内容
func Paser_low(table ...string) error{

    //判断参数是否正确
    if len(table) != 2 {
        return log.ALL_ERR("Wrong parse args")
    }

     //判断database文件夹是否存在
    _,err_f := os.Stat(fmt.Sprintf("./db/%s",table[0]))
    if err_f != nil {
        return log.ALL_ERR("No such database")
    }

     //判断file文件是否存在
    _,err_fi := os.Stat(fmt.Sprintf("./db/%s/%s.table",table[0],table[1]))
    if err_fi != nil {
        return log.ALL_ERR("No such table")
    }

    // 解析操作

    // 读取表头 规定第一行是表头 用|分隔字段名
    // 如username|password|permi ssion
    // 你知道的 这里应该是bufio
    var file,err = os.OpenFile(fmt.Sprintf("./db/%s/%s.table",table[0],table[1]),os.O_RDONLY,os.ModePerm)
    if err != nil {
        //到这里还能文件读取出错?
        return log.ALL_ERR("Can't open table file")
    }

    defer file.Close()

    var table_reader = bufio.NewReader(file)
    //单行读取 先读取第一行数据
    var data,_,err_tr = table_reader.ReadLine()
    if err_tr != nil {
        if err_tr == io.EOF {
            log.STDLOG("An empty table",fmt.Sprintf("./db/%s/%s.table",table[0],table[1]))
            return nil
        }
        return log.ALL_ERR("Read table file failed")
    }

    //解析获得的字符串
    var data_parsed []string = strings.Split(string(data),"|")

    //TODO 创建某种结构体来按照字符串进行索引分配
    println(data_parsed[0])
    return nil
}