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
	var commands, err_splt = Split(command)
	if err_splt != nil {
		return log.Runtime_log_err(&err.SyntaxError{
			Msg: "split commands failed",
		})
	}

	//小于四不可能是一个合法的SQL语句
	if len(commands) < 4 {
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
			//使用数据库的命令
		case "use", "USE", "Use":
			{
				if len(commands) == 3 && (commands[1] == "DATABASE" || commands[1] == "database" || commands[1] == "Database") {

					//查询一下有没有这个数据库
					if core.CheckDatabase(commands[2]) {
						user.Database = commands[2]
						fmt.Printf("switch to %s\n", commands[2])
					} else {
						fmt.Printf("No such database uhh\n")
					}

				} else {
					fmt.Printf("Unknown syntax. Please check your gosql version or typing \"help\" ~\n")
				}
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
		return excuteSQL(tree, user)
	}

	return log.Runtime_log_err(&err.SyntaxError{
		Msg: "Continue command line",
	})
}

func excuteSQL(tree *syntaxNode, user *Database_user) error {
	if user.Database == "" {
		return log.Runtime_log_err(&err.SyntaxError{
			Msg: "No database was used",
		})
	}

	//这一步传入的一定是一个完整的语法树
	switch tree.syntax_type {
	case SELECT:
		{
			//权限判断
			//从树里拿表和数据库我就不说了
			if core.PermissionCheck(user.User, user.Database, tree.left.value) {
				//fmt.Printf("%s\n", strings.Join(tree.value, " "))
				//fmt.Printf("%s\n", strings.Join(tree.left.value, " "))
				//调用Select
				//判断*的查询情况 这样就需要设置所有的Select选项
				var all = false
				var outputs string
				if len(tree.value) >= 1 && tree.value[0] == "*" {

					//var keys, err_lk = core.GetAllKeys(user.Database, tree.left.value[0])
					all = true
				}
				for _, tb := range tree.left.value {

					if all {
						var keys, err_lk = core.GetAllKeys(user.Database, tb)
						if err_lk != nil {
							log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't get all keys in select",
							})
						}
						tree.value = keys
					}

					if tree.right != nil {
						//存在where条件时
						var condition = make(map[string]any)
						var heads []string
						//var heads_index = 0
						len := len(tree.right.value)
						for i := 0; i < len; i += 2 {
							condition[tree.right.value[i]] = tree.right.value[i+1]
							//heads_index++
						}

						//添加头部
						heads = append(heads, tree.value...)

						//condition["password"] = "kali"
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var select_re, err_sel = core.Select(user.Database, tb, tree.value, condition, tree.right.or)
						//var select_re, err_sel = core.Select("information_schema", "user", tree.value, condition, false)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't select from table when sql excute",
							})
						}
						//处理返回的字符为可打印
						outputs += strings.Join(heads, " ") + "\n"

						for _, ot := range select_re {
							//自信大胆没有空指针访问
							outputs += strings.Join(ot, " ") + "\n"
						}
					} else {
						//不存在Where 那就是全部都要
						var condition = make(map[string]any)
						var heads []string
						
						condition["*"] = "*"

						//添加头部
						heads = append(heads, tree.value...)

						//condition["password"] = "kali"
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var select_re, err_sel = core.Select(user.Database, tb, tree.value, condition, false)
						//var select_re, err_sel = core.Select("information_schema", "user", tree.value, condition, false)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't select from table when sql excute",
							})
						}
						//处理返回的字符为可打印
						outputs += strings.Join(heads, " ") + "\n"

						for _, ot := range select_re {
							//自信大胆没有空指针访问
							outputs += strings.Join(ot, " ") + "\n"
						}
						core.Get_Access(user.User, tree.left.value[0])
					}
				}
				//看看outputs吧好孩子
				fmt.Printf("outputs:\n%v\n", outputs)

			} else {
				return log.Runtime_log_err(&err.PermissionError{
					Msg: "Permission delined",
				})
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
