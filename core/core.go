package core

import (
	"fmt"
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

	//最基本的user表,权限表
	//新建数据库
	Create_Database("infomation_schema")
	//新建表
	var user = make(map[string]string)
	user["username"] = "string"
	user["password"] = "string"
	Create_Table("infomation_schema", "user", user)
	var permission = make(map[string]string)
	permission["user"] = "string"
	permission["databases"] = "string[]"
	Create_Table("infomation_schema", "permission", permission)
	//Insert之前进行读取判断防止重复
	var r_c = make(map[string]any)
	r_c["username"] = "root"
	root, root_user_err := Select("infomation_schema", "user", []string{"username"}, r_c)
	if root_user_err == nil {
		if len(root) == 0 {
			//插入root用户
			var ins = make(map[string]any)
			ins["username"] = "root"
			ins["password"] = "root"
			Insert("infomation_schema", "user", ins)
		}
	}
	//TODO use database时赋值两个全局map来减少创建Writer和Reader 性能优化
}
