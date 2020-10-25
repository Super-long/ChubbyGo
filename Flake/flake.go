package Flake

import (
	"github.com/sony/sonyflake"
	"log"
	"math/rand"
	"time"
	"net"
)

/*
 * @brief: 通过雪花算法生成一个全局唯一的ID
 * @notes: 在协议层使用0作为初始值，显然使用flake生成的uuid不可能为零,因为时间戳已经设置好了
 */
// ps:测试代码出现问题的原因很简单,就是时间戳可能是一样的,这其实没什么好办法,测试代码手工配置最方便。
func GetSonyflake() uint64 {

	setting := sonyflake.Settings{}
	// 设置启动时间,最大化算法有效使用时间
	setting.StartTime = time.Date(2020, 10, 24, 10, 24, 10, 24, time.UTC)
	setting.MachineID = GetMachineIdentification
	flake := sonyflake.NewSonyflake(setting)
	id, err := flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	return id
}

/*
 * @brief: 根据本机网卡MAC地址和随机数拼出一个16位的机器标识
 * @notes: sony的雪花算法库默认的生成机器标示函数是使用内网IP算的,因为客户端在调用这个函数,现在基本都使用NET技术,很可能出现重复,所以需要修改生成机器标识函数.
 * 目前打算前6位为分别为mac地址六项的第一个数的最后一位之间互相异或,后十位为随机值,因为还有时间戳位,这样可以最大化保证全局不会出现唯一ID,但是其实还是有可能.
 * 且因为同一个主机生成的机器标示肯定还是一样的,所以测试代码还是只能手动改UUID.
 */
func GetMachineIdentification() (uint16, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		log.Println("ERROR : Error reading network interface. " + err.Error())
		// 需要先检查Error
		return 0, nil
	}
	var res uint16 = 0
	// 8c:16:45:36:2 e:c 4
	// 1  3  6  9  12  15
	var indexs = [6]uint32{0, 3, 6, 9, 12, 15}

	for _, inter := range interfaces {
		mac := inter.HardwareAddr.String() //获取本机MAC地址 17位字符串
		//fmt.Println("DEBUG : MAC = ", mac)
		if len(mac) == 17 {       // 8c:16:45:36:2e:c4
			for i := 0; i < 6; i++ {
				var temp uint8 = 0
				temp = mac[indexs[i]] & 1	// 取最后一位
				res ^= uint16(temp << i)
			}
		}
	}

	// 生成0到1023的随机数
	rand.Seed(time.Now().Unix())	// 用时间作为随机数种子
	high := rand.Intn(1023)		// 伪随机数

	res |= uint16(high<<6)
	//log.Printf("DEBUG : machine identification(%b)\n", res)
	return res, nil
}