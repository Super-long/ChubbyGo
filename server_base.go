package main

import (
	"HummingbirdDS/Connect"
	"fmt"
)
import _ "HummingbirdDS/Config"

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
