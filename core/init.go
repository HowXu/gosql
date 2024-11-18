package core

import (
	"fmt"
	"strings"
	"time"

	"github.com/HowXu/bptree"
	"github.com/HowXu/gosql/log"
)

func Init() {
	//debug.DEBUG = 0
	bptree.GLOBAL_DEBUG = false
	//debug.LogLevel = 0
	//log目录
	Create_Folder("./log")
	//log file
	var file, _ = Create_File(fmt.Sprintf("./log/%s.log", time.Now().Format("2006-01-02-15-04-05")))
	log.Init(file)

	log.File_SM_Log("Init Logs")

	//db 目录 这个目录下是数据库文件
	//新建
	Create_Folder("./db")

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
	//Insert
	var ins = make(map[string]any)
	ins["username"] = "神"
	ins["password"] = "god"
	//Insert("infomation_schema","user",ins)

	//TODO Update函数
	var condition = make(map[string]any)
	condition["username"] = "howxu"
	//var data_update = make(map[string]any)
	//data_update["password"] = "success"

	a, b := Select("infomation_schema", "user", []string{"username", "password"}, condition)
	if b == nil {
		for _,v := range a {
			log.STD_SM_Log(strings.Join(v, " "))
		}
	}

	var condition2 = make(map[string]any)
	condition2["username"] = "kali"
	//var data_update = make(map[string]any)
	//data_update["password"] = "success"

	c, d := Select("infomation_schema", "user", []string{"username", "password"}, condition2)
	if d == nil {
		for _,v := range c {
			log.STD_SM_Log(strings.Join(v, " "))
		}
	}

	//TODO use database时赋值两个全局map来减少创建Writer和Reader 性能优化
	//读取User信息
	data, _, err := paser_low("infomation_schema", "user")
	if err == nil {
		var record, err_data = data["username"].Find(2, true)
		if err_data == nil {
			log.STD_SM_Log(string(record.Value))
		}
	}
}
