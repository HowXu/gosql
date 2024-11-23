package syntax

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
	"github.com/chzyer/readline"
	"golang.org/x/term"
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

	//进入语法树解析
	var tree, err_crt = Create_syntax_tree(command)
	//语法树出不来说明有问题啊孩子
	if err_crt != nil {
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
						//权限判断
						if core.PermissionCheck(user.User, commands[2]) {
							user.Database = commands[2]
							fmt.Printf("switch to %s\n", commands[2])
						} else {
							fmt.Printf("Permissiom delined\n")
						}

					} else {
						fmt.Printf("No such database uhh\n")
					}

				} else {
					fmt.Printf("Unknown syntax. Please check your gosql version or typing \"help\" ~\n")
				}
			}
			//创建数据库和表
		case "Create", "CREATE", "create":
			{
				if len(commands) >= 3 {
					if commands[1] == "DATABASE" || commands[1] == "database" || commands[1] == "Database" {
						//创建数据库
						core.Create_Database(commands[2], user.User)
					} else if commands[1] == "TABLE" || commands[1] == "table" || commands[1] == "Table" {
						//创建表
						if user.Database == "" {
							fmt.Printf("No database was used\n")
						} else {
							if tree == nil {
								return log.Runtime_log_err(&err.SyntaxError{
									Msg: "Bad data type",
								})
							}
							//使用语法树解析 没错这个就是拿捏指针的自信
							core.Create_Table_No_Map(user.User, user.Database, tree.value[0], tree.left.value)
						}
					} else {
						fmt.Printf("No such thing to create\n")
					}
				} else {
					fmt.Printf("Not a standard create syntax\n")
				}
			}
			//打印可用数据库和数据表
		case "SHOW", "Show", "show":
			{
				if len(commands) == 2 {
					if commands[1] == "DATABASES" || commands[1] == "databases" || commands[1] == "Databases" {
						//打印可访问数据库 首先用户一定存在

						core.Get_Access("information_schema", "permission")
						core.Lock("information_schema", "permission")
						condition := make(map[string]any)
						condition["user"] = user.User
						gets, s_err := core.Select("information_schema", "permission", []string{"permits"}, condition, false)
						core.UnLock("information_schema", "permission")
						if s_err != nil {
							return log.Runtime_log_err(&err.SyntaxError{
								Msg: "Can't select when show databases",
							})
						}
						if len(gets) != 0 && len(gets[0]) != 0 {
							dbs := strings.Split(gets[0][0], ",")
							var results = make([][]string, len(dbs))
							var result_index = 0
							for _, v := range dbs {
								//自信指针访问
								results[result_index] = append(results[result_index], strings.Split(v, ".")[0])
								result_index++
							}
							//打印
							print(results, []string{commands[1]})
						} else {
							fmt.Printf("No database you can use\n")
						}
					} else if commands[1] == "TABLES" || commands[1] == "tables" || commands[1] == "Tables" {
						//打印数据库下表
						var results []string
						if user.Database == "" {
							fmt.Printf("No database was used, there is nothing\n")
						} else {
							//获取该目录下所有table文件名称
							dirPath := fmt.Sprintf("./db/%s", user.Database)
							err2 := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
								if err != nil {
									return err
								}
								if !info.IsDir() {
									// 获取文件名
									fileName := filepath.Base(path)
									// 去除文件扩展名
									results = append(results, strings.TrimSuffix(fileName, filepath.Ext(fileName)))
								}
								return nil
							})

							if err2 != nil {
								return log.Runtime_log_err(&err.SyntaxError{
									Msg: "Can't show tables",
								})
							}
							//打印表 和打印数据库差不多
							var results_t = make([][]string, len(results))
							var result_index_t = 0
							for _, v := range results {
								//自信指针访问
								results_t[result_index_t] = append(results_t[result_index_t], v)
								result_index_t++
							}
							//打印
							print(results_t, []string{commands[1]})
							//fmt.Printf("strings.Join(results):\n%v\n", strings.Join(results, "\n"))
						}
					} else {
						fmt.Printf("No such thing to show\n")
					}
				} else {
					fmt.Printf("No thing to show\n")
				}
			}

		case "passwd", "Passwd", "PASSWD":
			{
				//改密码
				//进入密码输入类型
				fd := int(os.Stdin.Fd())

				// 使标准输入的文件描述符进入原始模式，这样就不会显示输入的字符
				oldState, raw_err := term.MakeRaw(fd)
				if raw_err != nil {
					return log.ALL_ERR("Can't get the command line")
				}
				defer term.Restore(fd, oldState) // 确保在函数返回时恢复终端状态

				fmt.Print("Enter your new password:")
				// 读取密码
				i_password, i_err := term.ReadPassword(fd)
				if i_err != nil {
					return log.ALL_ERR("Can't read password from command line")
				}
				//第二次输入
				fmt.Print("\nEnter again:")
				// 读取密码
				i_password2, i_err2 := term.ReadPassword(fd)
				if i_err2 != nil {
					return log.ALL_ERR("Can't read password from command line")
				}

				//很简单的密码匹配问题
				if string(i_password) == string(i_password2) {
					//然后调用Update修改密码
					condition := make(map[string]any)
					condition["username"] = user.User
					data := make(map[string]any)
					data["password"] = string(i_password2)
					core.Get_Access("information_schema", "user")
					core.Lock("information_schema", "user")
					err_u := core.Update("information_schema", "user", condition, data, false)
					core.UnLock("information_schema", "user")
					if err_u != nil {
						return &err.SyntaxError{
							Msg: "Password update failed",
						}
					}
					fmt.Printf("\nPassword change success\n")
				} else {
					fmt.Printf("\nDifferent passwords during the two input\n")
				}
			}

		default:
			{
				fmt.Printf("Unknown syntax. Please check your gosql version or typing \"help\" ~\n")
			}
		}

		return &err.SyntaxError{
			Msg: "Continue Command line",
		}
	}

	return excuteSQL(tree, user)

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
			if core.PermissionCheck(user.User, user.Database) {
				//fmt.Printf("%s\n", strings.Join(tree.value, " "))
				//fmt.Printf("%s\n", strings.Join(tree.left.value, " "))
				//调用Select
				//判断*的查询情况 这样就需要设置所有的Select选项
				var all = false
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
						//打印
						print(select_re, heads)
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
						//打印
						print(select_re, heads)
					}
				}

			} else {
				return log.Runtime_log_err(&err.PermissionError{
					Msg: "Permission delined",
				})
			}
		}
	case DELETE:
		{
			if core.PermissionCheck(user.User, user.Database) {
				//调用Delete
				for _, tb := range tree.left.value {

					if tree.right != nil {
						//存在where条件时
						var condition = make(map[string]any)
						//var heads_index = 0
						len := len(tree.right.value)
						for i := 0; i < len; i += 2 {
							condition[tree.right.value[i]] = tree.right.value[i+1]
							//heads_index++
						}
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var err_sel = core.Delete(user.Database, tb, condition, tree.right.or)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't delete from table when sql excute",
							})
						}
						fmt.Printf("Done\n")
					} else {
						//不存在Where 那就是全部删掉
						var condition = make(map[string]any)
						condition["*"] = "*"
						//condition["password"] = "kali"
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var err_sel = core.Delete(user.Database, tb, condition, false)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't delete from table when sql excute",
							})
						}
						fmt.Printf("Done\n")
					}
				}

			} else {
				return log.Runtime_log_err(&err.PermissionError{
					Msg: "Permission delined",
				})
			}
		}
	case UPDATE:
		{
			//fmt.Printf("|%s|\n", strings.Join(tree.value, "|"))
			//fmt.Printf("|%s|\n", strings.Join(tree.left.value, "|"))

			if core.PermissionCheck(user.User, user.Database) {
				//调用UPDATE
				for _, tb := range tree.value {

					if tree.right != nil {
						//存在where条件时
						var condition = make(map[string]any)
						//var heads_index = 0
						len1 := len(tree.right.value)
						for i := 0; i < len1; i += 2 {
							condition[tree.right.value[i]] = tree.right.value[i+1]
							//heads_index++
						}
						//这里要构造一个data进去
						//存在where条件时
						var data = make(map[string]any)
						//var heads_index = 0
						len2 := len(tree.left.value)
						for i := 0; i < len2; i += 2 {
							data[tree.left.value[i]] = tree.left.value[i+1]
							//heads_index++
						}
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var err_sel = core.Update(user.Database, tb, condition, data, tree.right.or)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't delete from table when sql excute",
							})
						}
						fmt.Printf("Done\n")
					} else {
						//不存在Where 那就是全部删掉
						var condition = make(map[string]any)
						condition["*"] = "*"
						var data = make(map[string]any)
						//var heads_index = 0
						len2 := len(tree.left.value)
						for i := 0; i < len2; i += 2 {
							data[tree.left.value[i]] = tree.left.value[i+1]
							//heads_index++
						}
						//condition["password"] = "kali"
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var err_sel = core.Update(user.Database, tb, condition, data, false)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't delete from table when sql excute",
							})
						}
						fmt.Printf("Done\n")
					}
				}

			} else {
				return log.Runtime_log_err(&err.PermissionError{
					Msg: "Permission delined",
				})
			}
		}
	case INSERT:
		{

			//fmt.Printf("|%s|\n", strings.Join(tree.value, "|"))
			//fmt.Printf("|%s|\n", strings.Join(tree.left.value, "|"))

			if core.PermissionCheck(user.User, user.Database) {
				//调用UPDATE
				for _, tb := range tree.value {
					//这个不可能有where条件 如果出现了where就是错的
					if tree.right != nil {
						//存在where条件时
						return log.Runtime_log_err(&err.DatabaseError{
							Msg: "Unbelievable where existing o.O",
						})
					} else {

						//condition["password"] = "kali"
						core.Get_Access(user.Database, tb)
						core.Lock(user.Database, tb)
						var err_sel = core.Insert(user.Database, tb, tree.left.value)
						core.UnLock(user.Database, tb)
						if err_sel != nil {
							return log.Runtime_log_err(&err.DatabaseError{
								Msg: "Can't delete from table when sql excute",
							})
						}
						fmt.Printf("Done\n")
					}
				}

			} else {
				return log.Runtime_log_err(&err.PermissionError{
					Msg: "Permission delined",
				})
			}
		}
	}
	return &err.SyntaxError{
		Msg: "Continue command line",
	}
}

func print(in [][]string, head []string) {
	fmt.Printf("\nResult below:\n")
	//要求传入的字符串应该有一定的格式
	var max_lens []int
	var len_heads int = len(head)
	//最长值至少是head的长度
	for _, h := range head {
		max_lens = append(max_lens, len(h))
	}
	//理论上我们应该获得每种数据的最长一行

	for _, value := range in {
		//找到每种数据最长一行
		for i := 0; i < len_heads; i++ {
			if len(value[i]) > max_lens[i] {
				max_lens[i] = len(value[i])
			}
		}
	}
	//接下来构建打印序列
	var total_len = 1
	for _, i := range max_lens {
		total_len += i
		total_len++
	}
	var outputs string
	//先构建上层
	for i := 0; i < total_len; i++ {
		outputs += "-"
	}
	//加入头部
	outputs += "\n|"

	for i := 0; i < len_heads; i++ {
		//这样可以同时拿到max_lens和v1的数据
		var fix = max_lens[i] - len(head[i])
		outputs += head[i]
		for i := 0; i < fix; i++ {
			outputs += " "
		}
		outputs += "|"
	}

	outputs += "\n"
	//下层
	for i := 0; i < total_len; i++ {
		outputs += "-"
	}
	outputs += "\n"
	//加入数据
	for _, v1 := range in {
		outputs += "|"
		for i := 0; i < len_heads; i++ {
			//这样可以同时拿到max_lens和v1的数据
			var fix = max_lens[i] - len(v1[i])
			outputs += v1[i]
			for i := 0; i < fix; i++ {
				outputs += " "
			}
			outputs += "|"
		}
		outputs += "\n"
		for i := 0; i < total_len; i++ {
			outputs += "-"
		}
		outputs += "\n"
	}

	fmt.Print(outputs)

}
