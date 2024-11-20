package syntax

import (
	"strings"

	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
	"github.com/lvyahui8/goenum"
)

// 类型枚举
type NodeType struct {
	goenum.Enum
}

var (
	SELECT = goenum.NewEnum[NodeType]("SELECT")
	INSERT = goenum.NewEnum[NodeType]("INSERT")
	UPDATE = goenum.NewEnum[NodeType]("UPDATE")
	DELETE = goenum.NewEnum[NodeType]("DELETE")

	FROM  = goenum.NewEnum[NodeType]("FROM")
	SET   = goenum.NewEnum[NodeType]("SET")
	INTO  = goenum.NewEnum[NodeType]("INTO")
	WHERE = goenum.NewEnum[NodeType]("WHERE")
)

type syntaxNode struct {
	syntax_type NodeType
	left        *syntaxNode
	right       *syntaxNode
	value       []string
	and         bool
	or          bool
}

//这里的语法树最多支持到select * from table where a and b这种格式 因此只需要一个小型的语法树结构就可以了

// 构建语法树节点
func create_node(args []string, s_type NodeType) *syntaxNode {
	var node = new(syntaxNode)
	node.syntax_type = s_type
	node.value = args
	node.left = nil
	node.right = nil
	node.and = false
	node.or = false
	return node
}

// 构造语法树 或者说叫解析语法
func Create_syntax_tree(line string) (*syntaxNode, error) {
	//使用空格分开所有部分
	var head *syntaxNode
	var units = strings.Split(line, " ")
	//小于四不可能是一个合法的SQL语句
	if len(units) < 4 {
		return nil, log.Runtime_log_err(&err.SyntaxError{
			Msg: "Wrong parameters. Please check your sql sentences.",
		})
	}

	switch units[0] {
	case "SELECT", "Select", "select":
		{
			head = create_node(strings.Split(strings.TrimSpace(units[1]), ","), SELECT)
			if !(units[2] == "from" || units[2] == "FROM" || units[2] == "From") {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Error from location",
				})
			}
			head.left = create_node(strings.Split(strings.TrimSpace(units[3]), ","), FROM)
			if where_condition_proc(head, units, 4, 6) != nil {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Error Where conditionprocess",
				})
			}
		}
	case "DELETE", "Delete", "delete":
		{
			head = create_node([]string{}, DELETE)
			if !(units[1] == "from" || units[1] == "FROM" || units[1] == "From") {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Error from location",
				})
			}
			head.left = create_node(strings.Split(strings.TrimSpace(units[2]), ","), FROM)
			if where_condition_proc(head, units, 3, 5) != nil {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Error Where conditionprocess",
				})
			}
		}
	case "UPDATE", "Update", "update":
		{
			head = create_node([]string{}, UPDATE)
			head.value = strings.Split(strings.TrimSpace(units[1]), ",")
			if !(units[2] == "SET" || units[2] == "set" || units[2] == "Set") {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Wrong parameters. Please check your sql sentences.",
				})
			}
			//update set有一个专门的语法判定
			var updates []string
			for _, v := range strings.Split(strings.TrimSpace(units[3]), ",") {
				for _, k := range strings.Split(strings.TrimSpace(v), "=") {
					if k != "" {
						updates = append(updates, k)
					}
				}

			}
			//判断是不是双数
			//这里判断一下参数对不对 必须是双数
			//fmt.Printf("%d\n", len(updates))
			if len(updates)%2 != 0 {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Wrong parameters. Please check your sql sentences.",
				})
			}
			head.left = create_node(updates, SET)
			if where_condition_proc(head, units, 4, 6) != nil {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Error Where conditionprocess",
				})
			}
		}
	case "INSERT", "Insert", "insert":
		{
			head = create_node([]string{}, UPDATE)
			head.value = strings.Split(units[2], ",")
			if !(units[3] == "VALUES" || units[3] == "values" || units[3] == "Values") {
				return head, log.Runtime_log_err(&err.SyntaxError{
					Msg: "Wrong parameters. Please check your sql sentences.",
				})
			}

			//values嵌入
			var updates []string
			for _, v := range strings.Split(strings.TrimSpace(units[4]), ",") {

				if v != "" {
					updates = append(updates, v)
				}

			}

			head.left = create_node(updates, SET)

		}
	}

	return head, nil

}

// 封装处理Where条件的函数 语法树 语法单元 where出现的最小位置 完整的where语句的长度
func where_condition_proc(head *syntaxNode, units []string, where int, slen int) error {
	if len(units) >= slen && (units[where] == "WHERE" || units[where] == "where" || units[where] == "Where") {
		//大于5说明可能存在WHERE条件 需要对右节点进行操作
		//对第六参数进行操作
		for i := where + 1; i < len(units); i += 2 {
			//这个检测机制保证了不会是nil调用
			if head.right == nil {
				head.right = create_node([]string{}, WHERE)
			}

			if i+1 >= len(units) {
				//没有下一个条件就可以积极了 那就只处理当前
				for _, v := range strings.Split(strings.TrimSpace(units[i]), "=") {
					if v != "" {
						head.right.value = append(head.right.value, v)
					}

				}

				//head.right.value = append(head.right.value, strings.Split(units[i], "=")...)
				break
			}

			//存在一个and 或者 or 连接符号时
			if (units[i+1] == "AND" || units[i+1] == "And" || units[i+1] == "and") && !head.right.or {
				//进入and
				if !head.right.and {
					head.right.and = true
				}
			} else if (units[i+1] == "OR" || units[i+1] == "Or" || units[i+1] == "or") && !head.right.and {
				if !head.right.or {
					head.right.or = true
				}
			} else {
				return log.Runtime_log_err(&err.SyntaxError{
					Msg: "Wrong parameters. Please check your sql sentences.",
				})
			}

			//更新右节点的参数数量
			//注意这里要做空字符判定

			for _, v := range strings.Split(strings.TrimSpace(units[i]), "=") {
				if v != "" {
					head.right.value = append(head.right.value, v)
				}

			}

		}
		//这里判断一下参数对不对 必须是双数
		if len(head.right.value)%2 != 0 {
			return log.Runtime_log_err(&err.SyntaxError{
				Msg: "Wrong parameters. Please check your sql sentences.",
			})
		}
	}
	return nil
}
