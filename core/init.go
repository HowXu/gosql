package core

import (
	"fmt"
	"github.com/HowXu/gosql/log"
	"time"
)

func Init() {
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
	Create_File("./db/infomation_schema/user.table")
	Create_File("./db/infomation_schema/permission.table")
	//TODO 操作数据表的函数
}
