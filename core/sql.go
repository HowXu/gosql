package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/HowXu/bptree"
	"github.com/HowXu/gosql/log"
)

// 解析数据库文件内容
func Paser_low(database string, table string) (map[string]*bptree.Tree, []string, error) {
	var trees = make(map[string]*bptree.Tree)
	//data_parsed保留返回
	var data_parsed []string

	//判断database文件夹是否存在
	_, err_f := os.Stat(fmt.Sprintf("./db/%s", database))
	if err_f != nil {
		return trees, data_parsed, log.ALL_ERR("No such database")
	}

	//判断file文件是否存在
	_, err_fi := os.Stat(fmt.Sprintf("./db/%s/%s.table", database, table))
	if err_fi != nil {
		return trees, data_parsed, log.ALL_ERR("No such table")
	}

	// 解析操作

	// 读取表头 规定第一行是表头 用|分隔字段名
	// 如username|password|permi ssion
	// 你知道的 这里应该是bufio
	var file, err = os.OpenFile(fmt.Sprintf("./db/%s/%s.table", database, table), os.O_RDONLY, os.ModePerm)
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
			log.STDLOG("An empty table", fmt.Sprintf("./db/%s/%s.table", database, table))
			return trees, data_parsed, errors.New("Empty")
		}
		return trees, data_parsed, log.ALL_ERR("Read table file failed")
	}

	//接着读取第二行数据类型 这里直接放空
	var _, _, err_tp = table_reader.ReadLine()
	if err_tp != nil {
		if err_tp == io.EOF {
			log.STDLOG("An empty table", fmt.Sprintf("./db/%s/%s.table", database, table))
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
				log.STDLOG("Read table finish", table)
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

// 插入数据
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
				if v, ok := value.(float64); ok {
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
	var w_table_file, err_wt = os.OpenFile(table_path, os.O_APPEND|os.O_WRONLY, 0644)
	if err_wt != nil {
		return log.ALL_ERR("Can't write input to table file")
	}
	defer w_table_file.Close()
	var writer = bufio.NewWriter(w_table_file)
	writer.WriteString(strings.Join(input, "|") + "\n")
	return writer.Flush()
}

// 更新数据
func Update(database string, table string, condition map[string]any, data map[string]any) error {
	//插入前先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var _, stat = os.Stat(table_path)
	if stat != nil {
		return log.ALL_ERR("Update data to an unexist table")
	}

	//表存在,现在读取第二行
	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return log.ALL_ERR("Can't open table file when insert")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)
	//空读取第一行
	var keys, _, err_key = reader.ReadLine()
	if err_key != nil {
		return log.ALL_ERR("Read data type failed")
	}
	var keys_parsed = strings.Split(string(keys), "|")
	//读取第二行获取数据类型
	//这里支持的有string int float boolean string[]
	var types, _, err_rd = reader.ReadLine()
	if err_rd != nil {
		return log.ALL_ERR("Read data type failed")
	}
	var types_parsed = strings.Split(string(types), "|")

	//现在根据数据类型进行匹配
	//解析数据库文件
	var tree, _, err = Paser_low(database, table)
	if err != nil {
		return log.ALL_ERR("Parse failed when update data")
	}

	var target []int
	//这个方法防止target有重复元素
	addElement := func(v int) {
		for _, existing := range target {
			if existing == v {
				return // 元素已存在，不添加
			}
		}
		target = append(target, v) // 添加新元素
	}

	for c_key, c_value := range condition {
		//全页匹配数据库文件
		var key_index int
		//获取c_key的横向索引值
		for idx, ky := range keys_parsed {
			if ky == c_key {
				key_index = idx
				break
			} else {
				return log.ALL_LOG("No such key in table. unable to get condition")
			}

		}
		//类型匹配
		for i := 0; ; i++ {
			record, _ := tree[c_key].Find(i, true)
			if record == nil {
				break
			}
			switch types_parsed[key_index] {
			case "string", "string[]":
				{
					if v, ok := c_value.(string); ok {
						if string(record.Value) == v {
							//对上一个了,先存起来,到for循环外面进行进一步匹配
							addElement(i)
						}
					}

				}
			case "int":
				{
					if v, ok := c_value.(int); ok {
						//转换record
						var int_v, err = strconv.Atoi(string(record.Value))
						if err == nil {
							if int_v == v {
								addElement(i)
							}
						} else {
							return log.ALL_ERR("convert element from record to int when update. Maybe a type error?")
						}
					}
				}
			case "float":
				{
					if v, ok := c_value.(float64); ok {
						//转换record
						var float_v, err = strconv.ParseFloat(string(record.Value), 64)
						if err == nil {
							if float_v == v {
								addElement(i)
							}
						} else {
							return log.ALL_ERR("convert element from record to float when update. Maybe a type error?")
						}
					}
				}
			case "boolean":
				{
					if v, ok := c_value.(bool); ok {
						//转换record
						var boolean_v, err = strconv.ParseBool(string(record.Value))
						if err == nil {
							if boolean_v == v {
								addElement(i)
							}
						} else {
							return log.ALL_ERR("convert element from record to boolean when update. Maybe a type error?")
						}
					}
				}
			}
		}
	}

	var end_target []int
	//进一步匹配target
	for _, index := range target {

		var should_keep bool = true

		for c_key, c_value := range condition {
			var key_index int
			//获取c_key的横向索引值
			for idx, ky := range keys_parsed {
				if ky == c_key {
					key_index = idx
					break
				}
				return log.ALL_LOG("No such key in table. unable to get condition")
			}
			//找到对应c_key的其他键值
			record, _ := tree[c_key].Find(index, true)
			switch types_parsed[key_index] {
			case "string", "string[]":
				{
					if v, ok := c_value.(string); ok {
						if string(record.Value) != v {
							should_keep = false
						}
					}

				}
			case "int":
				{
					if v, ok := c_value.(int); ok {
						//转换record
						var int_v, err = strconv.Atoi(string(record.Value))
						if err == nil {
							if int_v != v {
								should_keep = false
							}
						} else {
							return log.ALL_ERR("convert element from record to int when update. Maybe a type error?")
						}
					}
				}
			case "float":
				{
					if v, ok := c_value.(float64); ok {
						//转换record
						var float_v, err = strconv.ParseFloat(string(record.Value), 64)
						if err == nil {
							if float_v != v {
								should_keep = false
							}
						} else {
							return log.ALL_ERR("convert element from record to float when update. Maybe a type error?")
						}
					}
				}
			case "boolean":
				{
					if v, ok := c_value.(bool); ok {
						//转换record
						var boolean_v, err = strconv.ParseBool(string(record.Value))
						if err == nil {
							if boolean_v != v {
								should_keep = false
							}
						} else {
							return log.ALL_ERR("convert element from record to boolean when update. Maybe a type error?")
						}
					}
				}
			}
		}
		//如果所有匹配模式符合,加入到最终匹配集
		if should_keep {
			end_target = append(end_target, index)
		}
	}

	//通过最终匹配集合,构造新的字符串
	//TODO 效率太低了吧每次更新数据都要重新读写文件
	var file, err_file = os.OpenFile(table_path, os.O_WRONLY, 0644)
	if err_file != nil {
		return log.ALL_ERR("Can't open table file when update")
	}
	defer file.Close()
	//上层的Reader直接在第三行开始了
	//用缓冲区我只能说性能更下一层楼
	var writer = bufio.NewWriter(file)
	//先把头和数据类型读进去
	writer.WriteString(strings.Join(keys_parsed, "|") + "\n")
	writer.WriteString(strings.Join(types_parsed, "|") + "\n")
	var line_index int = 0
	for _, index := range end_target {
		var result []string
		for type_index, index_key := range keys_parsed {
			//data key
			for key, c_value := range data {
				//TODO: 冷暴力处理
				var record, _ = tree[index_key].Find(index, true)

				switch types_parsed[type_index] {
				case "string", "string[]":
					{
						if v, ok := c_value.(string); ok {
							if key == index_key {
								result = append(result, v)
							} else {
								result = append(result, string(record.Value))
							}
						}

					}
				case "int":
					{
						if v, ok := c_value.(int); ok {
							if key == index_key {
								result = append(result, string(v))
							} else {
								result = append(result, string(record.Value))
							}
						}
					}
				case "float":
					{
						if v, ok := c_value.(float64); ok {
							if key == index_key {
								result = append(result, fmt.Sprintf("%f", v))
							} else {
								result = append(result, string(record.Value))
							}
						}
					}
				case "boolean":
					{
						if v, ok := c_value.(float64); ok {
							if key == index_key {
								result = append(result, fmt.Sprintf("%v", v))
							} else {
								result = append(result, string(record.Value))
							}
						}
					}
				}

			}
		}
		//接下来把字符串替换进去 这里的index从第三行开始计数为0
		for {
			//推进ReadLine函数
			if line_index == index {
				//对应行 则写入不同数据
				writer.WriteString(strings.Join(result, "|") + "\n")
				//补上空读一行
				reader.ReadLine()
				line_index++
				break
			} else {
				//非对应行 写入原始数据
				var out_read, _, err_rd_line = reader.ReadLine()
				if err_rd_line != nil {
					return log.ALL_ERR("Read other lins failed when update")
				}
				writer.WriteString(string(out_read) + "\n")
			}
			line_index++
		}
	}
	//别忘了剩下部分的数据
	for {
		var out_read, _, err_rd_line = reader.ReadLine()
		if err_rd_line == io.EOF {
			break
		}
		writer.WriteString(string(out_read) + "\n")
	}
	// 清空文件内容
	var err_clear = os.Truncate(table_path, 0)
	if err_clear != nil {
		return log.ALL_ERR("Can't clear table file when update")
	}
	//输出
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
