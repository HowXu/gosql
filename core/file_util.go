package core

import (
	"os"
	"github.com/HowXu/gosql/log"
)

func Create_Folder(path string) error {
	var err = os.Mkdir(path,os.ModePerm)
	var _,exsit = os.Stat(path)
	if exsit == nil {
		return nil
	}
	if err != nil {
		//说明文件夹存在或者权限问题 打印Log
		log.STDLOG("文件夹创建失败",path)
		//TODO 操作回退
		return err
	}
	return nil
}

func Create_File(path string) (*os.File,error){
	var file,err = os.Create(path)
	var _,exsit = os.Stat(path)
	if exsit == nil {
		return file,nil
	}
	if err != nil {
		log.STDLOG("文件创建失败",path)
		return file,err
	}
	defer file.Close()
	return file,nil
}