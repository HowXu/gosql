package com

import (
	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
)

// 命令语法 gosql -version 或者 gosql -u root
func Command(args []string) (string, error) {
	//解析输入的args并进行执行
	if len(args) == 1 {
		printHelp()
		return "", log.Runtime_log_err(&err.CommandError{
			Trap: false,
		})
	}
	//0为exe文件名称
	switch args[1] {
	case "-version":
		{
			show_version()
			return "", log.Runtime_log_err(&err.CommandError{
				Trap: false,
			})
		}
	case "-u":
		{
			//判断长度和有无-p参数
			if len(args) != 4 {
				log.STD_SM_Log("Wrong command format. it should be like \"gosql -u root -p\"")
				return "", log.Runtime_log_err(&err.CommandError{
					Trap: false,
				})
			}
			if args[3] != "-p" {
				log.STD_SM_Log("Wrong command format. it should be like \"gosql -u root -p\"")
				return "", log.Runtime_log_err(&err.CommandError{
					Trap: false,
				})
			}
			//查询用户信息
			var condition = make(map[string]any)
			condition["username"] = args[2]
			var result, err_se = core.Select("information_schema", "user", []string{"username", "password"}, condition, false)

			if err_se != nil {
				log.STD_SM_Log("Can't Query from user")
				return "", log.Runtime_log_err(&err.CommandError{
					Trap: false,
				})
			}
			if len(result) != 1 {
				log.STD_SM_Log("No this user or mutiple users")
				return "", log.Runtime_log_err(&err.CommandError{
					Trap: false,
				})
			}
			//这里单独做一下第一次登陆判断
			if result[0][1] == "root" {
				log.Runtime_Warn("you login as root.\nthe default password is root too.\nplease reset the root password")
			}
			//登录返回sql命令行
			return login(result[0][0], result[0][1])
		}
	case "-h":
		{
			printHelp()
			return "", log.Runtime_log_err(&err.CommandError{
				Trap: false,
			})
		}
	}

	return "", log.Runtime_log_err(&err.CommandError{
		Trap: false,
	})
}
