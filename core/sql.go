package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/HowXu/bptree"
	"github.com/HowXu/gosql/log"
)

// 解析数据库文件内容
func Paser_low(table ...string) (map[string]*bptree.Tree, []string, error) {
	var trees = make(map[string]*bptree.Tree)
	//data_parsed保留返回
	var data_parsed []string
	//var types_parsed []string
	//判断参数是否正确
	if len(table) != 2 {
		return trees, data_parsed, log.ALL_ERR("Wrong parse args")
	}

	//判断database文件夹是否存在
	_, err_f := os.Stat(fmt.Sprintf("./db/%s", table[0]))
	if err_f != nil {
		return trees, data_parsed, log.ALL_ERR("No such database")
	}

	//判断file文件是否存在
	_, err_fi := os.Stat(fmt.Sprintf("./db/%s/%s.table", table[0], table[1]))
	if err_fi != nil {
		return trees, data_parsed, log.ALL_ERR("No such table")
	}

	// 解析操作

	// 读取表头 规定第一行是表头 用|分隔字段名
	// 如username|password|permi ssion
	// 你知道的 这里应该是bufio
	var file, err = os.OpenFile(fmt.Sprintf("./db/%s/%s.table", table[0], table[1]), os.O_RDONLY, os.ModePerm)
	if err != nil {
		//到这里还能文件读取出错?
		return trees, data_parsed, log.ALL_ERR("Can't open table file")
	}

	defer file.Close()

	var table_reader = bufio.NewReader(file)
	//单行读取 先读取第一行数据
	var data, _, err_tr = table_reader.ReadLine()
	if err_tr != nil {
		if err_tr == io.EOF {
			log.STDLOG("An empty table", fmt.Sprintf("./db/%s/%s.table", table[0], table[1]))
			return trees, data_parsed, errors.New("Empty")
		}
		return trees, data_parsed, log.ALL_ERR("Read table file failed")
	}

	//接着读取第二行数据类型 这里直接放空
	var _, _, err_tp = table_reader.ReadLine()
	if err_tp != nil {
		if err_tp == io.EOF {
			log.STDLOG("An empty table", fmt.Sprintf("./db/%s/%s.table", table[0], table[1]))
			return trees, data_parsed, errors.New("Empty")
		}
		return trees, data_parsed, log.ALL_ERR("Read table file failed")
	}

	//types_parsed = strings.Split(string(types), "|")
	data_parsed = strings.Split(string(data), "|")
	//解析头 为每个头创建树

	for i := 0; i < len(data_parsed); i++ {
		trees[data_parsed[i]] = bptree.NewTree()
	}
	//接下来为每个tree添加数据

	//B+树存储部分
	var key int = 0
	for {
		//读取下一行
		var data_line, _, err_tr = table_reader.ReadLine()
		if err_tr != nil {
			if err_tr == io.EOF {
				log.STDLOG("Read table finish", table[1])
				break
			}
		}

		var data_parsed_line []string = strings.Split(string(data_line), "|")
		for i := 0; i < len(trees); i++ {
			//其实不用判断这一行数据对不对,因为插入时一定会保证正确
			trees[data_parsed[i]].Insert(key, []byte(data_parsed_line[i]))
		}
		key++
	}
	//最后获得了完整的b+tree数组,返回
	return trees, data_parsed, nil
}

func Insert(database string, table string, data map[string]any) error {
	//向表写入字段 调用这个函数一定是在Create之后,因此table file至少有两行

	//插入前先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var _, stat = os.Stat(table_path)
	if stat != nil {
		return log.ALL_ERR("Insert data to an unexist table")
	}

	//表存在,现在读取第二行
	var table_file, err = os.OpenFile(table_path, os.O_RDONLY, 0644)
	
	if err != nil {
		return log.ALL_ERR("Can't open table file when insert")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()


	var reader = bufio.NewReader(table_file)
	//空读取第一行
	reader.ReadLine()
	//读取第二行获取数据类型
	//这里支持的有string int float boolean string[]
	var types, _, err_rd = reader.ReadLine()
	if err_rd != nil {
		return log.ALL_ERR("Read data type failed")
	}
	var types_parsed = strings.Split(string(types), "|")
	//现在构造新的字符串插入到文件末尾
	var input []string
	var index int = 0
	for _, value := range data {
		// 尝试将any类型的值转换为string
		switch types_parsed[index] {
		case "string", "string[]":
			{
				if v, ok := value.(string); ok {
					input = append(input, v)
				} else {
					return log.ALL_ERR("Error string or string type")
				}
			}
		case "int":
			{
				if v, ok := value.(int); ok {
					input = append(input, string(v))
				} else {
					return log.ALL_ERR("Error int type")
				}
			}
		case "float":
			{
				if v, ok := value.(float32); ok {
					input = append(input, fmt.Sprintf("%f", v))
				} else {
					return log.ALL_ERR("Error float type")
				}
			}
		case "boolean":
			{
				if v, ok := value.(bool); ok {
					input = append(input, fmt.Sprintf("%v", v))
				} else {
					return log.ALL_ERR("Error boolean type")
				}
			}
		}
	}
	
	//如果没有问题,写入文件
	var w_table_file,err_wt = os.OpenFile(table_path,os.O_APPEND | os.O_WRONLY,0644)
	if err_wt != nil {
		return log.ALL_ERR("Can't write input to table file")
	}
	defer w_table_file.Close()
	var writer = bufio.NewWriter(w_table_file) 
	writer.WriteString(strings.Join(input,"|") + "\n")
	return writer.Flush()
}

func Create_Database(database string) error {
	return Create_Folder(fmt.Sprintf("./db/%s", database))
}

func Create_Table(database string, table string, head map[string]string) error {
	//传入应该有表头
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	if len(head) == 0 {
		return log.ALL_ERR("Empty table head")
	}

	Create_File_only(table_path)
	//写入表头

	//防止覆写
	var info, _ = os.Stat(table_path)
	if info.Size() != 0 {
		return log.ALL_ERR("Not an empty table when create")
	}

	var file, err = os.OpenFile(table_path, os.O_APPEND|os.O_WRONLY, 0644)
	defer file.Close()
	if err != nil {
		log.FileErr("Can't open table file", table)
	}

	var writer = bufio.NewWriter(file)

	//字符串拼接
	//把所有键拿到手
	var heads []string
	var types []string
	for key, value := range head {
		heads = append(heads, key)
		types = append(types, value)
	}

	var heads_output = strings.Join(heads, "|") + "\n"
	var types_output = strings.Join(types, "|") + "\n"
	//写入文件
	var _, err_r = writer.WriteString(heads_output)
	if err_r != nil {
		return log.ALL_ERR("Write table file failed")
	}
	var _, err_r2 = writer.WriteString(types_output)
	if err_r2 != nil {
		return log.ALL_ERR("Write table file failed")
	}
	writer.Flush()
	return nil
}
