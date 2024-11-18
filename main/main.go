package main

import (
	"os"

	"github.com/HowXu/gosql/com"
	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/syntax"
)

func main() {
	core.Init()
	//解析命令行参数并执行 
	u,how := com.Command(os.Args)
	//如果不是 Trap进入用户命令行
	if how == nil {
		//进入用户输入行模式
		var user = new(syntax.Database_user)
		user.Database = ""
		user.User = u
		syntax.OnUser(user)
	}
}
