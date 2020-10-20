package main

import "HummingbirdDS/Connect"
import _ "HummingbirdDS/Config"

func main(){
	cfg := Connect.CreatServer(3)
	cfg.StartServer()
	for true {

	}
	return
}
