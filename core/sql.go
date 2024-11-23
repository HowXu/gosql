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
	"github.com/HowXu/gosql/err"
	"github.com/HowXu/gosql/log"
	"github.com/HowXu/gosql/util"
)

// 解析数据库文件内容
func paser_low(database string, table string) (map[string]*bptree.Tree, []string, error) {
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
		//fmt.Printf("len(trees): %v\n", len(trees))
		//fmt.Printf("len(data_parsed_line): %v\n", len(data_parsed_line))
		//fmt.Printf("len(data_parsed): %v\n", len(data_parsed))
		//fmt.Printf("trees: %v\n", trees)
		//fmt.Printf("data_parsed_line: %v\n", data_parsed_line)
		//fmt.Printf("data_parsed: %v\n", data_parsed)
		for i := 0; i < len(trees); i++ {
			//其实不用判断这一行数据对不对,因为插入时一定会保证正确
			trees[data_parsed[i]].Insert(key, []byte(data_parsed_line[i]))
		}
		key++
	}
	//最后获得了完整的b+tree数组,返回
	return trees, data_parsed, nil
}

// 模式匹配的封装 含有模糊匹配选项即OR时
func match(database string, table string, condition map[string]any, usage string, is_arc bool) (string, []int, []string, []string, map[string]*bptree.Tree, error) {
	//先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var end_target []int
	var keys_parsed []string
	var types_parsed []string
	var tree map[string]*bptree.Tree

	var _, stat = os.Stat(table_path)
	if stat != nil {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("match data to an unexist table", usage)
	}

	//表存在,现在读取第二行
	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("Can't open table file when match", usage)
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)
	//空读取第一行
	var keys, _, err_key = reader.ReadLine()
	if err_key != nil {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("Read data type failed", usage)
	}
	keys_parsed = strings.Split(string(keys), "|")
	//读取第二行获取数据类型
	//这里支持的有string int float boolean string[]
	var types, _, err_rd = reader.ReadLine()
	if err_rd != nil {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("Read data type failed", usage)
	}
	types_parsed = strings.Split(string(types), "|")

	if len(keys_parsed) == 0 || len(types_parsed) == 0 {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("Zero kyes or types", usage)
	}

	//现在根据数据类型进行匹配
	//解析数据库文件
	tree, _, err := paser_low(database, table)
	if err != nil {
		return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("Parse failed when match data", usage)
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

	var no_condition = false

	for c_key, c_value := range condition {
		//判断是不是no_consition条件
		if c_key == "*" && c_value == "*" {
			no_condition = true
		}
		//全页匹配数据库文件
		var key_index int
		//获取c_key的横向索引值
		//这里有逻辑问腿啊bro
		var not_find bool = true
		for idx, ky := range keys_parsed {
			if ky == c_key {
				key_index = idx
				not_find = false
				break
			}
		}

		if not_find && !no_condition {
			return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("No such key in table. unable to get condition", usage)
		}

		//一个no_condition专门的匹配
		if no_condition {
			for i := 0; ; i++ {
				record, _ := tree[keys_parsed[0]].Find(i, true)
				if record == nil {
					break
				}
				addElement(i)
			}

		}

		//类型匹配和数据匹配
		for i := 0; ; i++ {
			if no_condition {
				break
			}
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
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to string when match. Maybe a type error?", usage)
					}

				}
			case "int":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.Atoi(p)
						var int_v, err = strconv.Atoi(string(record.Value))
						if err_atoi == nil && err == nil {
							if int_v == v {
								addElement(i)
							}
						} else {
							return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to int when match. Maybe a type error?", usage)
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to int when match. Maybe a type error?", usage)
					}

				}
			case "float":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.ParseFloat(p, 64)
						var float_v, err = strconv.ParseFloat(string(record.Value), 64)
						if err_atoi == nil && err == nil {
							if float_v == v {
								addElement(i)
							}
						} else {
							return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
					}
				}
			case "boolean":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.ParseBool(p)
						var boolean_v, err = strconv.ParseBool(string(record.Value))
						if err_atoi == nil && err == nil {
							if boolean_v && v {
								addElement(i)
							} else {
								return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
							}
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
					}

				}
			}
		}
	}

	//fmt.Printf("target: %v\n", target)

	//模糊匹配或者无限制时直接返回
	if is_arc || no_condition {
		return table_path, target, keys_parsed, types_parsed, tree, nil
	}

	//进一步匹配target
	for _, index := range target {

		var should_keep bool = true

		for c_key, c_value := range condition {
			var key_index int
			//获取c_key的横向索引值
			//这里有逻辑问腿啊bro
			var not_find bool = true
			for idx, ky := range keys_parsed {
				if ky == c_key {
					key_index = idx
					not_find = false
					break
				}
			}
			if not_find {
				return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("No such key in table. unable to get condition", usage)
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
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to string when match. Maybe a type error?", usage)
					}

				}
			case "int":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.Atoi(p)
						var int_v, err = strconv.Atoi(string(record.Value))
						if err_atoi == nil && err == nil {
							if int_v != v {
								should_keep = false
							}
						} else {
							return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to int when match. Maybe a type error?", usage)
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to int when match. Maybe a type error?", usage)
					}

				}
			case "float":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.ParseFloat(p, 64)
						var float_v, err = strconv.ParseFloat(string(record.Value), 64)
						if err_atoi == nil && err == nil {
							if float_v != v {
								should_keep = false
							}
						} else {
							return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
					}
				}
			case "boolean":
				{
					//先断言成string
					if p, ok := c_value.(string); ok {
						v, err_atoi := strconv.ParseBool(p)
						var boolean_v, err = strconv.ParseBool(string(record.Value))
						if err_atoi == nil && err == nil {
							if boolean_v != v {
								should_keep = false
							} else {
								return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
							}
						}
					} else {
						return table_path, end_target, keys_parsed, types_parsed, tree, log.ALL_ATA_ERR("convert element from record to float when match. Maybe a type error?", usage)
					}

				}
			}
		}
		//如果所有匹配模式符合,加入到最终匹配集
		if should_keep {
			end_target = append(end_target, index)
		}
	}
	//匹配完全,返回需要的数据
	return table_path, end_target, keys_parsed, types_parsed, tree, nil
}

// 插入数据
func Insert(database string, table string, data []string) error {
	//向表写入字段 调用这个函数一定是在Create之后,因此table file至少有两行

	//插入前先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var _, stat = os.Stat(table_path)
	if stat != nil {
		return log.ALL_ERR("Insert data to an unexist table")
	}

	//表存在,现在读取第二行
	var table_file, err1 = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err1 != nil {
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
	//这里要判断传入的长度和types是否相同
	if len(types_parsed) != len(data) {
		return &err.SyntaxError{
			Msg: "Not suitable datas",
		}
	}
	for _, value := range data {
		// 尝试将any类型的值转换为string
		switch types_parsed[index] {
		case "string", "string[]":
			{
				input = append(input, value)
			}
		case "int":
			{

				v, err_atoi := strconv.Atoi(value)
				if err_atoi == nil {
					input = append(input, strconv.FormatInt(int64(v), 10))
				} else {
					return log.ALL_ERR("Error int type")
				}

			}
		case "float":
			{
				//先断言成string
				v, err_atoi := strconv.ParseFloat(value, 64)
				if err_atoi == nil {
					input = append(input, fmt.Sprintf("%f", v))
				} else {
					return log.ALL_ERR("Error float type")
				}

			}
		case "boolean":
			{
				//先断言成string
				v, err_atoi := strconv.ParseBool(value)
				if err_atoi == nil {
					input = append(input, strconv.FormatBool(bool(v)))
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
func Update(database string, table string, condition map[string]any, data map[string]any, is_arc bool) error {
	//插入前匹配
	var table_path, end_target, keys_parsed, types_parsed, tree, err = match(database, table, condition, "update", is_arc)

	if err != nil {
		return err
	}

	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return log.ALL_ERR("Can't open table file when update")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)
	//跳过前两行
	reader.ReadLine()
	reader.ReadLine()

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
	//fmt.Printf("keys_parsed: %v\n", keys_parsed)
	writer.WriteString(strings.Join(types_parsed, "|") + "\n")
	var line_index int = 0
	for _, index := range end_target {
		var result []string
		for type_index, index_key := range keys_parsed {
			//data key
			for key, c_value := range data {
				//TODO: 冷暴力处理
				var record, _ = tree[index_key].Find(index, true)
				//fmt.Printf("string(record.Value): %v\n", string(record.Value))
				switch types_parsed[type_index] {
				case "string", "string[]":
					{
						if key == index_key {
							if v, ok := c_value.(string); ok {
								result = append(result, v)
							}
						} else {
							result = append(result, string(record.Value))
						}

					}
				case "int":
					{
						if key == index_key {
							//先断言成string
							if p, ok := c_value.(string); ok {
								v, err_atoi := strconv.Atoi(p)
								if err_atoi == nil {
									fmt.Printf("strconv.FormatInt(int64(v), 10): %v\n", strconv.FormatInt(int64(v), 10))
									result = append(result, strconv.FormatInt(int64(v), 10))
								}
							}

						} else {
							result = append(result, string(record.Value))
						}

					}
				case "float":
					{
						if key == index_key {
							//先断言成string
							if p, ok := c_value.(string); ok {
								v, err_atoi := strconv.ParseFloat(p, 64)
								if err_atoi == nil {
									fmt.Printf("fmt.Sprintf(\"f\", v): %v\n", fmt.Sprintf("%f", v))
									result = append(result, fmt.Sprintf("%f", v))
								}
							}
						} else {
							result = append(result, string(record.Value))
						}

					}
				case "boolean":
					{
						//你应该先比较key
						if key == index_key {
							//fmt.Printf("case boolean\n")
							if p, ok := c_value.(string); ok {
								//fmt.Printf("invoke\n")
								v, err_atoi := strconv.ParseBool(p)
								if err_atoi == nil {
									//fmt.Printf("do i done 1 ?")
									//fmt.Printf("strconv.FormatBool(bool(v)): %v\n", strconv.FormatBool(bool(v)))
									result = append(result, strconv.FormatBool(bool(v)))

								}
							}
						} else {
							//fmt.Printf("do i done 2 ?")
							result = append(result, string(record.Value))
						}
						//先断言成string

					}
				}

			}
		}
		//fmt.Printf("result: %v\n", result)
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

// 删除数据
func Delete(database string, table string, condition map[string]any, is_arc bool) error {
	//删除前先判断表是否存在
	//插入前匹配
	var table_path, end_target, keys_parsed, types_parsed, _, err = match(database, table, condition, "delete", is_arc)

	if err != nil {
		return err
	}

	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return log.ALL_ERR("Can't open table file when update")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)
	//跳过前两行
	reader.ReadLine()
	reader.ReadLine()

	//通过最终匹配集合,构造新的字符串
	//TODO 效率太低了吧每次更新数据都要重新读写文件
	var file, err_file = os.OpenFile(table_path, os.O_WRONLY, 0644)
	if err_file != nil {
		return log.ALL_ERR("Can't open table file when delete")
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
		//接下来把字符串替换进去 这里的index从第三行开始计数为0
		for {
			//推进ReadLine函数
			if line_index == index {
				//对应行 写入空
				//writer.WriteString(strings.Join(result, "|") + "\n")
				//补上空读一行
				reader.ReadLine()
				line_index++
				break
			} else {
				//非对应行 写入原始数据
				var out_read, _, err_rd_line = reader.ReadLine()
				if err_rd_line != nil {
					return log.ALL_ERR("Read other lins failed when delete")
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
		return log.ALL_ERR("Can't clear table file when delete")
	}
	//输出
	return writer.Flush()

}

// 查询数据 返回{{"kali","howxu"},{"range","frank"}}
func Select(database string, table string, need []string, condition map[string]any, is_arc bool) ([][]string, error) {
	var result [][]string
	//插入前匹配
	var table_path, end_target, _, _, tree, err = match(database, table, condition, "select", is_arc)
	//fmt.Printf("end_target: %v\n", end_target)
	if err != nil {
		return result, err
	}

	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return result, log.ALL_ERR("Can't open table file when select")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)
	//空读两行
	reader.ReadLine()
	reader.ReadLine()
	//遍历end_target
	for _, index := range end_target {
		//根据需要的字符串拿东西
		//创建一整行
		var in []string

		for _, needed := range need {
			//拿tree里的东西 每一行都有一个完整的

			var te, ex = tree[needed]
			if !ex {
				return result, log.ALL_ERR("Wrong key access. No this need")
			}
			var record, _ = te.Find(index, true)
			if record == nil {
				return result, log.ALL_LOG("Can't call find to get a record")
			}
			in = append(in, string(record.Value))
		}
		result = append(result, in)
	}

	return result, nil
}

// 新建数据库
func Create_Database(database string, user string) error {
	//直接判断存不存在
	var _, exsit = os.Stat(fmt.Sprintf("./db/%s", database))
	if exsit == nil {
		return log.Runtime_log_err(&err.DatabaseError{Msg: "Exsiting databse"})
	}
	//如果是root应该直接允许 记得给自己权限
	if user == "root" {
		//拿取root当前的信息
		var condition_r = make(map[string]any)
		condition_r["user"] = "root"
		Lock("information_schema", "permission")
		var gets_r, se_err_r = Select("information_schema", "permission", []string{"permits"}, condition_r, false)
		UnLock("information_schema", "permission")
		if se_err_r != nil {
			return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't select from permission when create database"})
		}
		var getss_r []string
		getss_r = (strings.Split(gets_r[0][0], ","))
		var data_r = make(map[string]any)
		data_r["permits"] = strings.Join(append(getss_r, database+".*"), ",")
		Get_Access("information_schema", "permission")
		Lock("information_schema", "permission")
		var err_up = Update("information_schema", "permission", condition_r, data_r, false)
		UnLock("information_schema", "permission")
		if err_up != nil {
			return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't update from permission when create database"})
		}
		return util.Create_Folder(fmt.Sprintf("./db/%s", database))
	}
	//这样百分百不存在

	//最后创建文件夹
	return create_database_User(database, user)
}

// 不管创建什么数据库 都有我root的权限
func create_database_User(database string, user string) error {
	//不只是简单的直接创建 创建调用的用户应该要被更新到permission里面
	Get_Access("information_schema", "permission")
	//拿取这个用户当前的信息
	var condition = make(map[string]any)
	condition["user"] = user
	Lock("information_schema", "permission")
	var gets, se_err = Select("information_schema", "permission", []string{"permits"}, condition, false)
	UnLock("information_schema", "permission")
	if se_err != nil {
		return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't select from permission when create database"})
	}

	//拿取root当前的信息
	var condition_r = make(map[string]any)
	condition_r["user"] = "root"
	Lock("information_schema", "permission")
	var gets_r, se_err_r = Select("information_schema", "permission", []string{"permits"}, condition_r, false)
	UnLock("information_schema", "permission")
	if se_err_r != nil {
		return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't select from permission when create database"})
	}

	//加进去 因为这个数据库不存在所以这一步肯定是加入一个全新的元素
	var getss []string
	var getss_r []string
	if len(gets) > 0 && len(gets[0]) > 0 {
		getss = (strings.Split(gets[0][0], ","))
	}
	getss_r = (strings.Split(gets_r[0][0], ","))

	var data = make(map[string]any)
	data["permits"] = strings.Join(append(getss, database+".*"), ",")
	var data_r = make(map[string]any)
	data_r["permits"] = strings.Join(append(getss_r, database+".*"), ",")
	//接下来塞回去
	if len(gets) > 0 && len(gets[0]) > 0 {
		Get_Access("information_schema", "permission")
		Lock("information_schema", "permission")
		var err_up = Update("information_schema", "permission", condition, data, false)
		UnLock("information_schema", "permission")

		if err_up != nil {
			return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't update from permission when create database"})
		}
	} else {
		//0应该Insert
		Get_Access("information_schema", "permission")
		Lock("information_schema", "permission")
		var err_up = Insert("information_schema", "permission", []string{user, strings.Join(append(getss, database+".*"), ",")})
		UnLock("information_schema", "permission")

		if err_up != nil {
			return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't update from permission when create database"})
		}
	}

	Get_Access("information_schema", "permission")
	Lock("information_schema", "permission")
	var err_up = Update("information_schema", "permission", condition_r, data_r, false)
	UnLock("information_schema", "permission")
	if err_up != nil {
		return log.Runtime_log_err(&err.DatabaseError{Msg: "Can't update from permission when create database"})
	}

	return util.Create_Folder(fmt.Sprintf("./db/%s", database))
}

// 请确保调用该函数时head长度为2的倍数
func Create_Table_No_Map(user string, database string, table string, head []string) error {

	//接下来判定一下这个表的权限是不是到位了
	//如果是root应该直接放行
	if user == "root" {
		return create_table_User_No_Map(database, table, head)
	}

	var cn = make(chan bool)
	go PermissionCheck(user, database,cn)
	//true说明允许创建表
	if !<-cn {
		return &err.DatabaseError{Msg: "Permission delined when create table"}
	}

	return create_table_User_No_Map(database, table, head)

}

// 请确保调用该函数时head长度为2的倍数
func create_table_User_No_Map(database string, table string, head []string) error {
	//传入应该有表头
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	if len(head) == 0 {
		return log.ALL_ERR("Empty table head")
	}

	util.Create_File_only(table_path)
	//写入表头

	//防止覆写
	var info, _ = os.Stat(table_path)
	if info.Size() != 0 {
		return log.ALL_LOG("Not an empty table when create")
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
	//只需要改变一下for循环就是一个专用的创建表
	for i := 0; i < len(head); i += 2 {
		heads = append(heads, head[i])
		types = append(types, head[i+1])
	}

	fmt.Printf("heads: %v\n", heads)
	fmt.Printf("types: %v\n", types)

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

// 新建表
func Create_Table(user string, database string, table string, head map[string]string) error {

	//接下来判定一下这个表的权限是不是到位了
	//如果是root应该直接放行
	if user == "root" {
		return create_table_User(database, table, head)
	}

	var cn = make(chan bool)
	go PermissionCheck(user, database,cn)
	//true说明允许创建表
	if !<-cn {
		return &err.DatabaseError{Msg: "Permission delined when create table"}
	}

	return create_table_User(database, table, head)

}

func create_table_User(database string, table string, head map[string]string) error {
	//传入应该有表头
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	if len(head) == 0 {
		return log.ALL_ERR("Empty table head")
	}

	util.Create_File_only(table_path)
	//写入表头

	//防止覆写
	var info, _ = os.Stat(table_path)
	if info.Size() != 0 {
		return log.ALL_LOG("Not an empty table when create")
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

	fmt.Printf("heads: %v\n", heads)
	fmt.Printf("types: %v\n", types)

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

func GetAllTypes(database string, table string) ([]string, error) {
	//先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var types_parsed []string

	var _, stat = os.Stat(table_path)
	if stat != nil {
		return types_parsed, log.ALL_ERR("match data to an unexist table")
	}

	//表存在,现在读取第二行
	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return types_parsed, log.ALL_ERR("Can't open table file when match")
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
		return types_parsed, log.ALL_ERR("Read data type failed")
	}

	types_parsed = strings.Split(string(types), "|")
	return types_parsed, nil
}

func GetAllKeys(database string, table string) ([]string, error) {
	//先判断表是否存在
	var table_path = fmt.Sprintf("./db/%s/%s.table", database, table)
	var keys_parsed []string

	var _, stat = os.Stat(table_path)
	if stat != nil {
		return keys_parsed, log.ALL_ERR("match data to an unexist table")
	}

	//表存在,现在读取第二行
	var table_file, err_p = os.OpenFile(table_path, os.O_RDONLY, 0644)

	if err_p != nil {
		return keys_parsed, log.ALL_ERR("Can't open table file when match")
	}

	//读完并且构造字符串后关掉文件 防止中途return
	defer table_file.Close()

	var reader = bufio.NewReader(table_file)

	var types, _, err_rd = reader.ReadLine()
	if err_rd != nil {
		return keys_parsed, log.ALL_ERR("Read keys failed")
	}

	keys_parsed = strings.Split(string(types), "|")
	return keys_parsed, nil
}
