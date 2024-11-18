package com

import (
	"fmt"
	"os"

	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
	"golang.org/x/term"
)

func show_version() {
	fmt.Printf("gosql version %s\npowered by github.com/HowXu && Golang", core.Version)
}

func login(user string, password string) (string, error) {
	//Linux格式的密码输入
	fmt.Print("Enter your password:")
	fd := int(os.Stdin.Fd())

	// 使标准输入的文件描述符进入原始模式，这样就不会显示输入的字符
	oldState, raw_err := term.MakeRaw(fd)
	if raw_err != nil {
		return user, log.ALL_ERR("Can't get the command line")
	}
	defer term.Restore(fd, oldState) // 确保在函数返回时恢复终端状态

	// 读取密码
	i_password, i_err := term.ReadPassword(fd)
	if i_err != nil {
		return user, log.ALL_ERR("Can't read password from command line")
	}

	//很简单的密码匹配问题
	if string(i_password) == password {
		//Trap到onUser状态
		//log.Runtime_Log("Login success!")
		return user, nil
	}

	log.Runtime_Log("Wrong password!")

	return user, &err.CommandError{
		Trap: false,
	}
}

func printHelp() {
	show_version()
	fmt.Printf("\n\nUsage Below:\n\t-version\tShow version\n\t-u\t\tLogin with a [username] like \"gosql -u root\"\n\t-p\t\tUse password to sign in. It needn't parameter\n\t")
	fmt.Printf("-h\t\tShow this help information")
}
