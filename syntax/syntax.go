package syntax

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
)

type Database_user struct {
	Database string
	User     string
}

func OnUser(user *Database_user) error {
	//语法解析和处理
	// 创建一个新的bufio.Reader对象，它包装了os.Stdin
	terminal_reader := bufio.NewReader(os.Stdin)

	//打印gosql的提示符
	fmt.Printf("\n\nGosql version %s\n", core.Version)
	for {
		//命令提示行
		fmt.Print("gosql>")
		//重复输入
		read_command, err := terminal_reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			return log.ALL_ERR("Can't read from terminal")
		}

		// 去除读取到的字符串末尾的换行符
		read_command = read_command[:len(read_command)-1]
		//传递给语法解析
		exit := onSyntaxInput(user, read_command)
		if exit == nil {
			break
		}
	}
	return nil
}

func onSyntaxInput(user *Database_user, command string) error {
	//解析语法
	//首先判断长度
	if len(command) == 0 {
		return &err.SyntaxError{
			Msg: "Empty command",
		}
	}

	//按照空格分组
	var commands []string = strings.Split(command, " ")
	if len(commands) == 1 {
		//只有单个命令
		//为什么这里有"\0"
		var no_0 string = commands[0][:len(commands[0])-1]
		switch no_0 {
		case "exit":
			{
				fmt.Printf("bye bye ~\n")
				return nil
			}
		case "whoami":
			{
				fmt.Printf("%s\n",user.User)
			}
		}

	}

	return &err.SyntaxError{
		Msg: "Continue command line",
	}
}
