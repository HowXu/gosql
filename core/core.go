package core

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/HowXu/bptree"
	"github.com/HowXu/gosql/debug"
	"github.com/HowXu/gosql/log"
	"github.com/HowXu/gosql/util"
)

var Version string = "1.0-alpha"

func Init() {
	bptree.GLOBAL_DEBUG = false
	debug.LogLevel = 1
	//log目录
	util.Create_Folder("./log")
	//log file
	var file, _ = util.Create_File(fmt.Sprintf("./log/%s.log", time.Now().Format("2006-01-02-15-04-05")))
	log.Init(file)

	log.File_SM_Log("Init Logs")

	//db 目录 这个目录下是数据库文件
	//新建
	util.Create_Folder("./db")

	//表同步锁文件
	util.Create_File_only("table.lock")
	var lockfile, err_lock = os.OpenFile("table.lock", os.O_RDWR, os.ModePerm)
	if err_lock != nil {
		log.ALL_ERR("Can't access to lock file")
		return
	}
	LockFile = lockfile
	LockFilReader = bufio.NewReader(lockfile)
	LockFilWriter = bufio.NewWriter(lockfile)

	//最基本的user表,权限表
	//新建数据库
	Create_Database("information_schema")
	//新建表
	var user = make(map[string]string)
	user["username"] = "string"
	user["password"] = "string"
	Create_Table("information_schema", "user", user)
	var permission = make(map[string]string)
	permission["user"] = "string"
	permission["databases"] = "string[]"
	Create_Table("information_schema", "permission", permission)
	//Insert之前进行读取判断防止重复
	var r_c = make(map[string]any)
	r_c["username"] = "root"
	Get_Access("information_schema", "user")
	Lock("information_schema", "user")
	root, root_user_err := Select("information_schema", "user", []string{"username"}, r_c)
	UnLock("information_schema", "user")
	if root_user_err == nil {
		if len(root) == 0 {
			//插入root用户
			var ins = make(map[string]any)
			ins["username"] = "root"
			ins["password"] = "root"
			Get_Access("information_schema", "user")
			Lock("information_schema", "user")
			Insert("information_schema", "user", ins)
			UnLock("information_schema", "user")
		}
	}
	//Insert之前进行读取判断防止重复
	var p_c = make(map[string]any)
	p_c["user"] = "root"
	Get_Access("information_schema", "permission")
	Lock("information_schema", "permission")
	per, root_per_err := Select("information_schema", "permission", []string{"user"}, p_c)
	UnLock("information_schema", "permission")
	if root_per_err == nil {
		if len(per) == 0 {
			//插入root的权限表
			var ins = make(map[string]any)
			ins["user"] = "root"
			ins["databases"] = "permission,user"
			Get_Access("information_schema", "permission")
			Lock("information_schema", "permission")
			Insert("information_schema", "permission", ins)
			UnLock("information_schema", "permission")
		}
	}

	//TODO use database时赋值两个全局map来减少创建Writer和Reader 性能优化
}
