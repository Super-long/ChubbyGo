package main

import (
	"ChubbyGo/Connect"
	"fmt"
	"time"
)
import _ "ChubbyGo/Config"

// import  _ "net/http/pprof"

func main(){

/*	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()*/

	cfg := Connect.CreatServer(3)
	err :=  cfg.StartServer()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for true {
		time.Sleep(20*time.Second)
	}
	return
}
