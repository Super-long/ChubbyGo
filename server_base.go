package main

import (
	"ChubbyGo/Connect"
	"fmt"
)
import _ "ChubbyGo/Config"

func main(){
	cfg := Connect.CreatServer(3)
	err :=  cfg.StartServer()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for true {

	}
	return
}
