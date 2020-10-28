package main

import (
	"HummingbirdDS/BaseServer"
	_ "HummingbirdDS/Config"
	"HummingbirdDS/Connect"
	"fmt"
	"log"
	"time"
)

/*
 * 测试分为两个部分,第一部分对于写锁测试,第二部分对于读锁测试
 */

func main() {
	n := 10	// n大于零
	Sem := make(Connect.Semaphore, n-1)
	SemNumber := 0

	clientConfigs := make([]*Connect.ClientConfig,n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		} else {
			clientConfigs[i].SetUniqueFlake(uint64(i+n*2))
		}
	}

	ok, fd := clientConfigs[0].Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd sucess, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!")
	}

	filename := "text.txt"
	// 在打开的文件夹下创建文件
	flag, fileFd := clientConfigs[0].Create(fd, BaseServer.PermanentFile,filename)
	if flag {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d\n", filename,fileFd.InstanceSeq)
	} else {
		fmt.Printf("Create Error!")

		// 防止有人把最底下的Delete参数定义为Opclose找不到错误原因赖在我头上
		flag = clientConfigs[0].Delete(fileFd, BaseServer.Opdelete)
		if flag {
			fmt.Printf("Delete (%s) sucess.\n", fileFd.PathName)
		} else {
			fmt.Println("Release Error!")
		}
		return
	}

	// 多个协程只有一个能获取写锁成功,当然这并不是绝对的;这里会有N-1个协程进入
	for i := 1; i < n; i++{
		SemNumber++
		go func (index int){
			defer Sem.P(1)
			flag ,Fd := clientConfigs[index].Open("/ls/ChubbyCell_lizhaolong/text.txt")
			if flag {
				fmt.Printf("Get fd sucess, instanceSeq is %d\n", Fd.InstanceSeq)
			} else {
				fmt.Println("Error!")
			}

			time.Sleep(1*time.Second)

			Isok, token := clientConfigs[index].Acquire(Fd, BaseServer.WriteLock)
			if Isok {
				fmt.Printf("Acquire (%s) sucess, Tocken is %d\n", filename, token)
				//time.Sleep(1*time.Second)

				Isok = clientConfigs[index].Release(Fd, token)
				if Isok {
					fmt.Printf("Release (%s) sucess.\n", Fd.PathName)
				} else {
					fmt.Println("Release Error!")
				}
			} else {
				fmt.Println("Acquire Error!")
			}

			// close操作,不删除文件,这个Delete可能比较迷惑,但是为了功能实现简单一点,加参数是个很好的选择
			flag = clientConfigs[index].Delete(Fd, BaseServer.Opclose)
			if flag {
				fmt.Printf("Delete (%s) sucess.\n", Fd.PathName)
			} else {
				fmt.Println("Release Error!")
			}
		}(i)
	}

	Sem.V(SemNumber)

	fmt.Println("----------------------------------------------")

	// 使用0号客户端再次请求锁,得到的token应该为1
	Isok, token := clientConfigs[0].Acquire(fileFd, BaseServer.WriteLock)
	if Isok {
		fmt.Printf("Acquire (%s) sucess, Token is %d\n", filename, token)
		//time.Sleep(1*time.Second)

		Isok = clientConfigs[0].Release(fileFd, token)
		if Isok {
			fmt.Printf("Release (%s) sucess.\n", fileFd.PathName)
		} else {
			fmt.Println("Release Error!")
		}
	} else {
		fmt.Println("Acquire Error!")
	}

	fmt.Println("----------------------------------------------")
	SemNumber = 0

	for i := 1; i < n; i++{
		SemNumber++
		go func (index int){
			defer Sem.P(1)
			flag ,Fd := clientConfigs[index].Open("/ls/ChubbyCell_lizhaolong/text.txt")
			if flag {
				fmt.Printf("Get fd sucess, instanceSeq is %d\n", Fd.InstanceSeq)
			} else {
				fmt.Println("Error!")
			}

			var Isok bool
			var token uint64

			time.Sleep(1*time.Second)

			// 读锁先抢到就是4个sucess; 写锁抢到就是1个sucess
			if index&1 == 0{
				Isok, token =  clientConfigs[index].Acquire(Fd, BaseServer.ReadLock)
			} else {
				Isok, token =  clientConfigs[index].Acquire(Fd, BaseServer.WriteLock)
			}

			if Isok {
				fmt.Printf("Acquire (%s) sucess, Token is %d\n", filename, token)
				//time.Sleep(1*time.Second)

				Isok = clientConfigs[index].Release(Fd, token)
				if Isok {
					fmt.Printf("Release (%s) sucess.\n", Fd.PathName)
				} else {
					fmt.Println("Release Error!")
				}
			} else {
				fmt.Println("Acquire Error!")
			}

			// close操作,不删除文件,这个Delete可能比较迷惑,但是为了功能实现简单一点,加参数是个很好的选择
			flag = clientConfigs[index].Delete(Fd, BaseServer.Opclose)
			if flag {
				fmt.Printf("Delete (%s) sucess.\n", Fd.PathName)
			} else {
				fmt.Println("Delete Error!")
			}
		}(i)
	}

	Sem.V(SemNumber)

	// 改成Opclose再一次执行就会发现创建文件失败了; 这里设置删除是为了多次跑这个程序
	flag = clientConfigs[0].Delete(fileFd, BaseServer.Opdelete)
	if flag {
		fmt.Printf("Delete (%s) sucess.\n", fileFd.PathName)
	} else {
		fmt.Println("Release Error!")
	}

}