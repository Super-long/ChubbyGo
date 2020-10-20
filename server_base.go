package main

import "HummingbirdDS/Connect"

func main(){
	cfg := Connect.CreatServer(3)
	cfg.StartServer()
	for true {

	}
	return
}
