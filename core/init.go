package core

import (
	"fmt"
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
	var file,_ = Create_File(fmt.Sprintf("./log/%s.log",time.Now().Format("2006-01-02-15-04-05")))
	log.Init(file)

	log.File_SM_Log("Init Logs")

	//db 目录 这个目录下是数据库文件
	Create_Folder("./db")
	
	//最基本的user表,权限表
	Create_Folder("./db/infomation_schema")
	Create_File_only("./db/infomation_schema/user.table")
	Create_File_only("./db/infomation_schema/permission.table")
	//TODO Insert函数
	data,_,err := Paser_low("infomation_schema","user")
	if err == nil {
		var record,_ =data["username"].Find(2,true)
		log.STD_SM_Log(string(record.Value))
	}
}
