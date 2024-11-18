package com

import (
	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
)

//命令语法 gosql -version 或者 gosql -u root
func Command(args []string) error {
	//解析输入的args并进行执行
	if len(args) == 1 {
		return nil
	}
	//0为exe文件名称
	switch args[1] {
	case "-version":
		{
			show_version()
			return nil
		}
	case "-u":
		{
			//判断长度和有无-p参数
			if len(args) != 4 {
				log.STD_SM_Log("Wrong command format. it should be like \"gosql -u root -p\"")
				return nil
			}
			if args[3] != "-p" {
				log.STD_SM_Log("Wrong command format. it should be like \"gosql -u root -p\"")
				return nil
			}
			//查询用户信息
			var condition = make(map[string]any)
			condition["username"] = args[2]
			var result,err = core.Select("infomation_schema","user",[]string{"username","password"},condition)
			if err != nil {
				log.STD_SM_Log("Can't Query from user")
				return nil
			}
			if len(result) != 1 {
				log.STD_SM_Log("No this user or table includes mutiple users")
				return nil
			}
			login(result[0][0],result[0][1])
		}
	}

	return &err.CommandError{
		Trap: false,
	}
}
