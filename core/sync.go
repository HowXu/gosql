package core

import (
	"bufio"
	"io"
	"os"
	"time"

	"github.com/HowXu/gosql/log"
)

//表同步锁

// 经过初始化表文件一定存在 直接使用进程全局变量减少OpenFile操作
var LockFile *os.File
var LockFilReader *bufio.Reader
var LockFilWriter *bufio.Writer

func Lock(database string, table string) error {
	//向表中加入table
	//首先读取表文件
	var tables []string
	for {
		var a_table, _, err_rd = LockFilReader.ReadLine()
		if err_rd == io.EOF {
			break
		}
		if err_rd != io.EOF && err_rd != nil {
			return log.ALL_ERR("Can't read next line when lock table")
		}
		tables = append(tables, string(a_table))
	}
	//遍历表文件是否存在该元素
	var exist bool = false
	for _, tb := range tables {
		if tb == database+"."+table {
			exist = true
			break
		}
	}

	// 获取文件状态信息
	//fileInfo, err := LockFile.Stat()
	//if err != nil {
	//	return log.ALL_ERR("Can't get file info when lock")
	//}

	if exist {
		return nil
	} else {
		//追加读取
		//_, err_sk := LockFile.Seek(fileInfo.Size(), io.SeekEnd)
		//if err_sk != nil {
		//	return log.ALL_ERR("Can't seek to end in lock file")
		//}
		LockFilWriter.WriteString(database + "." + table + "\n")
	}

	LockFilWriter.Flush()
	//回到开头
	_, err_sk2 := LockFile.Seek(0, io.SeekStart)
	return err_sk2
}

func UnLock(database string, table string) error {
	//去掉table
	//首先读取表文件
	var tables []string
	for {
		var a_table, _, err_rd = LockFilReader.ReadLine()
		if err_rd == io.EOF {
			break
		}
		if err_rd != io.EOF && err_rd != nil {
			return log.ALL_ERR("Can't read next line when unlock table")
		}
		tables = append(tables, string(a_table))
	}
	//遍历表文件是否存在该元素
	var location int = -1
	for index, tb := range tables {
		if tb == database+"."+table {
			location = index
			break
		}
	}

	// 获取文件状态信息
	//fileInfo, err := LockFile.Stat()
	//if err != nil {
	//	return log.ALL_ERR("Can't get file info when lock")
	//}

	if location == -1 {
		//这个表没有被lock 不需要解锁
		return log.ALL_LOG("This table needn't unlock")
	} else {
		//重新构造lock file
		//通过直接跳来减少内存使用
		for idx, tab := range tables {
			if idx != location {
				LockFilWriter.WriteString(tab)
			}
		}
		//清空源文件
		var err_clear = os.Truncate("table.lock", 0)
		if err_clear != nil {
			return log.ALL_ERR("Can't clear table file when unlock")
		}
	}

	LockFilWriter.Flush()
	//回到开头
	_, err_sk2 := LockFile.Seek(0, io.SeekStart)
	return err_sk2
}

func GetLockStat(database string, table string) bool {
	//获取Lock信息
	//首先读取表文件
	var tables []string
	var exist bool = false
	for {
		var a_table, _, err_rd = LockFilReader.ReadLine()
		if err_rd == io.EOF {
			break
		}
		if err_rd != io.EOF && err_rd != nil {
			log.ALL_ERR("Can't read next line when get lock stat")
			return false
		}
		tables = append(tables, string(a_table))
	}

	//遍历表文件是否存在该元素

	for _, tb := range tables {
		if tb == database+"."+table {
			exist = true
			break
		}
	}

	return exist
}

func Get_Access(database string, table string) {
	//通用的检测机制
	if GetLockStat(database, table) {
		//如果表被锁了
		for {
			//线程暂停1秒后再去看
			time.Sleep(1 * time.Second)
			if !GetLockStat(database, table) {
				//直到解锁
				break
			}
		}
	}
}
