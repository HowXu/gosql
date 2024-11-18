package main

import (
	"os"

	"github.com/HowXu/gosql/com"
	"github.com/HowXu/gosql/core"
)

func main() {
	core.Init()
	//解析命令行参数并执行 
	how := com.Command(os.Args)
	//如果不是 Trap进入用户命令行
	if how != nil {
		//进入用户输入行模式
		com.OnUser()
	}
}
