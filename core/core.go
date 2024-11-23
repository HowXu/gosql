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

var Version string = "1.1"
var plainText = "56We55qE5L2O6K+t"

func Init() {
	bptree.GLOBAL_DEBUG = false
	debug.LogLevel = 0
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
	//新建数据库 注意这里是有问题的 没有两个表文件是不可能创建数据库的
	util.Create_Folder("./db/information_schema")
	//Create_Database("information_schema","root")
	//只能使用原始IO
	//新建表
	Create_Table_No_Map("root","information_schema", "user",[]string{"username","string","password","string"})
	Create_Table_No_Map("root","information_schema", "permission", []string{"user","string","permits","string[]"})
	//Insert之前进行读取判断防止重复
	var r_c = make(map[string]any)
	r_c["username"] = "root"
	Get_Access("information_schema", "user")
	Lock("information_schema", "user")
	root, root_user_err := Select("information_schema", "user", []string{"username"}, r_c,false)
	UnLock("information_schema", "user")
	if root_user_err == nil {
		if len(root) == 0 {
			//插入root用户
			
			Get_Access("information_schema", "user")
			Lock("information_schema", "user")
			Insert("information_schema", "user", []string{"root","root"})
			UnLock("information_schema", "user")
		}
	}
	//Insert之前进行读取判断防止重复
	var p_c = make(map[string]any)
	p_c["user"] = "root"
	Get_Access("information_schema", "permission")
	Lock("information_schema", "permission")
	//对于第一次运行这里有一个有趣的循环效应
	per, root_per_err := Select("information_schema", "permission", []string{"user"}, p_c,false)
	UnLock("information_schema", "permission")
	if root_per_err == nil {
		if len(per) == 0 {
			//插入root的权限表
			Get_Access("information_schema", "permission")
			Lock("information_schema", "permission")
			Insert("information_schema", "permission", []string{"root","information_schema.*"})
			UnLock("information_schema", "permission")
		}
	}

	//TODO use database时赋值两个全局map来减少创建Writer和Reader 性能优化
}
