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
func Paser_low(table ...string) (map[string]*bptree.Tree,[]string,error) {
    var trees = make(map[string]*bptree.Tree)
    var data_parsed []string
	//判断参数是否正确
	if len(table) != 2 {
		return trees,data_parsed,log.ALL_ERR("Wrong parse args")
	}

	//判断database文件夹是否存在
	_, err_f := os.Stat(fmt.Sprintf("./db/%s", table[0]))
	if err_f != nil {
		return trees,data_parsed,log.ALL_ERR("No such database")
	}

	//判断file文件是否存在
	_, err_fi := os.Stat(fmt.Sprintf("./db/%s/%s.table", table[0], table[1]))
	if err_fi != nil {
		return trees,data_parsed,log.ALL_ERR("No such table")
	}

	// 解析操作

	// 读取表头 规定第一行是表头 用|分隔字段名
	// 如username|password|permi ssion
	// 你知道的 这里应该是bufio
	var file, err = os.OpenFile(fmt.Sprintf("./db/%s/%s.table", table[0], table[1]), os.O_RDONLY, os.ModePerm)
	if err != nil {
		//到这里还能文件读取出错?
		return trees,data_parsed,log.ALL_ERR("Can't open table file")
	}

	defer file.Close()

	var table_reader = bufio.NewReader(file)
	//单行读取 先读取第一行数据
	var data, _, err_tr = table_reader.ReadLine()
	if err_tr != nil {
		if err_tr == io.EOF {
			log.STDLOG("An empty table", fmt.Sprintf("./db/%s/%s.table", table[0], table[1]))
			return trees,data_parsed,errors.New("Empty")
		}
		return trees,data_parsed,log.ALL_ERR("Read table file failed")
	}

	data_parsed = strings.Split(string(data), "|")
	//解析头 为每个头创建树
	
	for i := 0; i < len(data_parsed); i++ {
		trees[data_parsed[i]] = bptree.NewTree()
	}
	//接下来为每个tree添加数据

	//data_parsed保留返回
	

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
            trees[data_parsed[i]].Insert(key,[]byte(data_parsed_line[i]))
        }
        key++
	}
    //最后获得了完整的b+tree数组,返回
	return trees,data_parsed,nil
}
