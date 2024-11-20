package syntax

import (
	"fmt"
	"strings"

	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
	"github.com/chzyer/readline"
)

type Database_user struct {
	Database string
	User     string
}

func OnUser(user *Database_user) error {
	//语法解析和处理
	//打印gosql的提示符
	fmt.Printf("\n\nGosql version %s\n", core.Version)
	for {
		//命令提示行
		fmt.Print("gosql>")
		//重复输入
		//read_command, err := terminal_reader.ReadString('\n')
		rl, err := readline.New("gosql>")
		if err != nil {
			panic(err)
		}
		defer rl.Close()

		line, err := rl.Readline()
		if err != nil {
			return log.ALL_ERR("Can't read from terminal")
		}
		// 去除读取到的字符串末尾的换行符
		//line = line[:len(line)-1]
		//传递给语法解析
		exit := onSyntaxInput(user, line)
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
		return log.Runtime_log_err(&err.SyntaxError{
			Msg: "Empty command",
		})
	}

	//按照空格分组
	var commands []string = strings.Split(command, " ")
	if len(commands) == 1 {
		//只有单个命令
		switch commands[0] {
		case "exit":
			{
				fmt.Printf("\nbye bye ~\n")
				return nil
			}
		case "whoami":
			{
				fmt.Printf("%s\n", user.User)
			}
		default:
			{
				fmt.Printf("Unknown syntax. Please check your gosql version or typing \"help\" ~\n")
			}
		}

	} else {
		//进入语法树解析
		var tree, err_crt = Create_syntax_tree(command)
		if err_crt != nil {
			return log.Runtime_log_err(&err.SyntaxError{
				Msg: "Create syntax tree failed",
			})
		}
		return excuteSQL(tree)
	}

	return log.Runtime_log_err(&err.SyntaxError{
		Msg: "Continue command line",
	})
}

func excuteSQL(tree *syntaxNode) error {
	//这一步传入的一定是一个完整的语法树
	switch tree.syntax_type {
	case SELECT:
		{
			//fmt.Printf("%s\n", strings.Join(tree.value, " "))
			fmt.Printf("%s\n", strings.Join(tree.left.value, " "))
			if tree.right != nil {
				if tree.right != nil {
					fmt.Printf("%s\n", strings.Join(tree.right.value, " "))
				}
			}

		}
	case DELETE:
		{
			//fmt.Printf("%s\n", strings.Join(tree.value, " "))
			fmt.Printf("|%s|\n", strings.Join(tree.left.value, "|"))
			if tree.right != nil {
				if tree.right != nil {
					fmt.Printf("%s\n", strings.Join(tree.right.value, "|"))
				}
			}
		}
	case UPDATE:
		{
			fmt.Printf("|%s|\n", strings.Join(tree.value, "|"))
			fmt.Printf("|%s|\n", strings.Join(tree.left.value, "|"))
			if tree.right != nil {
				if tree.right != nil {
					fmt.Printf("%s\n", strings.Join(tree.right.value, "|"))
				}
			}
		}
	case INSERT:
		{
			fmt.Printf("|%s|\n", strings.Join(tree.value, "|"))
			fmt.Printf("|%s|\n", strings.Join(tree.left.value, "|"))
		}
	}
	return log.Runtime_log_err(&err.SyntaxError{
		Msg: "Continue command line",
	})
}
