package core

import (
	//"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
)

// 判断是否有权限进行操作
func PermissionCheck(user string, database string, table []string) bool {
	//首先读取permission表
	Get_Access("information_schema", "permission")
	Lock("information_schema", "permission")
	var condition = make(map[string]any)
	condition["user"] = user
	var gets, err_g = Select("information_schema", "permission", []string{"permits"}, condition, false)
	UnLock("information_schema", "permission")
	if err_g != nil {
		log.Runtime_log_err(&err.PermissionError{
			Msg: "Can't select databases from permission table",
		})
		return false
	}
	//简单的遍历判断
	if len(gets) < 1 || len(gets[0]) < 1 {
		log.Runtime_log_err(&err.PermissionError{
			//这怎么可能? 没有这个用户不可能登录 防止内存Hook
			Msg: "No such user or no any permission in permission table",
		})
		return false
	}
	//接下来判断
	rt := true
	for _, tb := range table {
		var target = database + "." + tb
		var in_circle = false
		for _, v := range strings.Split(gets[0][0], ",") {
			if v == target {
				in_circle = true
				break
			}
		}
		if in_circle {
			continue
		}
		rt = false
	}

	return rt
}

// 查询数据库是否存在
func CheckDatabase(database string) bool {
	var get_dirs = func() ([]string, error) {
		var subdirectories []string
		err := filepath.Walk("./db", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && path != "./db" { // 排除根目录本身
				subdirectories = append(subdirectories, path)
			}
			return nil
		})
		return subdirectories, err
	}
	var drs, errg = get_dirs()
	if errg == nil {
		for _, v := range drs {
			//fmt.Printf("drs: %v\n", drs)
			if v == "db\\"+database {
				return true
			}
		}
	} else {
		log.Runtime_log_err(&err.PermissionError{
			//这怎么可能? 没有这个用户不可能登录 防止内存Hook
			Msg: "Can't get sub dirs in db",
		})
	}
	return false
}
