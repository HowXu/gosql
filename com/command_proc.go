package com

import (
	"fmt"

	"github.com/HowXu/gosql/core"
	"github.com/HowXu/gosql/log"
)

func show_version() {
	fmt.Printf("gosql version %s\npowered by github.com/HowXu && Golang",core.Version)
}

func login(user string,password string){
	log.STD_SM_Log("Login success!")
}