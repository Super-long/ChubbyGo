
# ChubbyGo

[外链图片转存失败,源站可能有防盗链机制,建议将图片保存下来直接上传(img-Ap69YJYO-1604219339346)(Pictures/ChubbyGo.jpg)]
This cute Totoro's name is Go, and he may be a friend of Tux!
这个可爱的龙猫的名字叫做Go，他也许是Tux的朋友！

## 1.Description

要运行ChubbyGo需要引入以下两个三方库：
1. github.com/sony/sonyflake
2. github.com/OneOfOne/xxhash

## 2.Introduction

ChubbyGo是一个基于**Raft协议**的分布式锁服务，其提供了在低耦合分布式系统中**粗粒度的锁定**和**可靠的存储**。

大家也许都知道Chubby其实是2006年谷歌发表的论文《The Chubby lock service for loosely-coupled distributed systems》中描述的服务，但是是闭源的，后来雅虎研究院将一个类似的项目捐献给了Apache基金会，这个项目的名字叫做ZooKeeper，并于2010年11月正式成为Apache的顶级项目。

为更高级别的抽象，以实现一个分布式协调中心，以此留给客户端更多的自由，但相应的带来了复杂度。为了更简单的使用，我根据论文实现了Chubby，并命名为**ChubbyGo**。

ChubbyGo与Chubby的目标一样，都是为了对中等规模的客户端的提供易于理解的语义与小规模可靠的存储。你可以使用ChubbyGo完成以下工作:
1. **为多台服务器进行可靠的选主**，因为最多只有一个服务器可以得到锁。
2. **对在多主机之间需要保护的资源提供分布式锁服务**。
3. **允许Master在ChubbyGo上发布消息，客户端可以使用get(key)得到消息**，key为用户定义，推荐使用master得到锁的路径作为key保证唯一性。
4. 本身可以存储少量的信息，所以可以作为**nameserver**。
5. 本身提供get/set接口，可以作为一个**可靠的键值服务器**。

## 3.Implement
因为我的精力与水平有限，暂时没办法实现论文上的全部细节，且一些地方为了能更为简洁与简单的实现，选择了和论文中不同的策略，本节中描述分为四个部分:
1. **与Chubby目前设计的不同之处**;
2. **目前已经完成的工作**;
3. **后续需要修改的已发现的问题与未实现的功能**;
4. **ChubbyGo文件系统中定义的有效行为**;

### 3.1 设计的不同点
1. Chubby采用Paxos作为底层一致性协议。ChubbyGo采用Raft作为底层一致性协议;
2. Chubby中客户端连接主服务器时存在一个DNS服务器，在其中可以解析出目前存在的主服务器。为了实现更为简单，目前采用客户端直接连接全部的主服务器的方法，follower节点对于客户端请求不进行应答，以此选主，当然客户端存在缓存，所以在绝大多数时候这并不是大问题。

### 3.2 已完成的工作：

1. raft基础协议完成，包括：**领导人选举，日志复制，日志压缩**；
2. 基于raft协议完成**强一致性的键值服务**，支持get/put请求;
3. 服务器之间运行前的连接：修改为超时时间倍增的重传，以抹平各机器服务器启动的时间差;
4. 配置项提取出来，配置文件使用json;
5. 出现环状引用：使用**Listener模式**解决;
6. 使用**服务器拒绝服务**使得多服务器同步启动协议，见RaftKV.ConnectIsok，集群部署成功;
7. **客户端实现在连接大于等于N/2+1机器时提供服务，而且在连接大多数服务器之后剩下未连接服务器;
会不断重连，当大于N/2+1的机器超时时客户端直接退出**，报错为超时;
8. flake算法初始机器标识符生成函数是拿内网IP计算，客户端使用会有很大问题，所以加入了**自定义的机器标识符生成函数**;
9. 持久化完成，**模仿Redis提供三种策略**:always,everysec,no;
10. 允许服务器宕机重启，并读取持久化数据，并重新加入原集群，整个**集群继续对外提供服务**;
11. 实现**类Unix文件系统接口**以提供编程人员更熟悉的**分布式锁**接口;
12. 引入ChubbyGoMap，**允许根据客户根据预期的请求类型动态的配置线程安全的hashmap类型**以提升并发度;
13. 支持不经过Raft层的fastget请求，此请求不保证任何程度的一致性，但在多读情况下有百分之三十左右的性能提升;

### 3.3 后续需要修改的已发现的问题与未实现的功能
这一部分分为两个部分描述，第一部分是已实现的部分还存在的缺陷，第二部分则为论文中描述的功能但暂未实现。
1. raft层需要实现集群关系变更，以支持动态扩展;
2. 在ChubbyGoFileSystem中创建文件的时候可以引入对于URL做文件名的URL解析(lockserver.go -> struct FileOperation)。
3. 在处理Raft层处发现条件竞争，暂定策略为修改文件系统节点定义，使全部文件系统操作不需要其他锁保护(Processdamen.go Acquire处理逻辑)。
4. 文件系统的权限定义目前尙不明确，即FileSystemNode中的ACLs部分，如何组织权限关系是一个需要思考的问题。
5. 后面是否支持目录的递归删除，因为目录下的文件可能还存在锁(fileSystem.go -> func Delete)。
6. ChubbyGoFileSystem中对于锁的操作可能导致写锁饥饿(fileSystem.go -> func Release)。
7. Acquire的超时参数到服务器端，需要计算包的传播时延和时钟不同步的度，当然服务器大一点对正确性影响并不大。
8. 创建文件时可以引入一个计数器，类似于信号量机制，以支持秒杀。
---
1. 不支持客户端订阅事件。
2. 不支持数据的客户端缓存。
3. 不支持会话与Keepalive，但使用另一种方法一定程度上保证分布式锁的安全，或者说一个时刻最多一个客户端操作资源。

对于以上三点需要解释一下，因为目前底层通信采用Golang自带的RPC库，这使得其实很难去进行一个双工的通信，从功能的角度来讲就是很难去做到服务器去主动请求客户端。

因为这需要服务器去建立连接，这显然是不合理的。使用C++可以很容易的做到这一点，因为服务器维护这客户端的fd，随时可以进行通信，只需要保证和epoll内的逻辑处理不冲突就ok了，但是Golang我目前不太清楚如何实现这一点。

### 3.4 ChubbyGo文件系统中定义的行为：
1. **"/ls"为所有ChubbyGo Cell的初始根目录**。
2. **open可以打开一个文件或者文件夹，并返回一个文件描述符**。
3. **我们可以使用一个目录的文件描述符在其中create一个文件或者文件夹，默认open，并返回文件描述符**。
4. **open操作会增加文件或者目录的引用计数**。
5. **使用完毕需要使用close(delete加参数)操作**。
6. **close可以减小文件的引用计数，临时文件当计数为零时自动删除，永久文件和文件夹则不会删除**。
7. **delete可以加参数，当文件引用计数为零的时候直接删除任意文件或者目录**。
8. **可以对每一个ChubbyGo Cell内除了根目录以外的目录加锁，即根目录可以打开，无法被加锁**。
9. **当打开一个文件时我们可以对其进行加锁操作，支持读锁和写锁**。
10. **加锁以后需要解锁**。

## 4. 配置文件说明

### 4.1 server_config.json
1. **servers_address**: 对端服务器地址。
2. **myport**: 本身的端口。
3. **maxreries**: 服务器启动最大重试数。
4. **timeout_entry**: 每次启动的间隔时间，单位为毫秒。
5. **maxraftstate**: 日志的最大存储字节总量，达到阈值百分之八十五时进行日志压缩。
6. **snapshotfilename**: 快照的文件名。
7. **raftstatefilename**: Raft信息持久化的文件名。
8. **persistencestrategy**: 持久化策略，分为三种"always","everysec","no"。
9. **chubbygomapstrategy**: 线程安全的哈希map的，分为两种"concurrentmap","syncmap"。

### 4.2 client_config.json
1. **client_address**: 对端服务器的地址。
2. **maxreries** : client连接大于一半的服务器的最大重试数。

## 5.部署
多台服务器之间必须清楚对端的地址，这个信息应该写入在 **"ChubbyGo/Config/server_config.json"** 的 **"servers_address"** 字段。客户端应该在 **"ChubbyGo/Config/client_config.json"** 的 **"client_address"** 字段中写入这N台服务器的地址。**N推荐为奇数**。

当然也可以配置其他的参数，具体见第四节。

我们使用三个进程模拟三个服务器，一个进程模拟一个客户端，确保8900，8901，8902三个端口没有使用。

服务器进程A修改配置文件如下：
```json
{
  "servers_address": ["localhost:8900","localhost:8901"],
  "myport" : ":8902",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot1.hdb",
  "raftstatefilename" : "Persister/raftstate1.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}
```

服务器进程B修改配置文件如下：
```json
{
  "servers_address": ["localhost:8900","localhost:8902"],
  "myport" : ":8901",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot2.hdb",
  "raftstatefilename" : "Persister/raftstate2.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}
```
服务器进程C修改配置文件如下：
```json
{
  "servers_address": ["localhost:8901","localhost:8902"],
  "myport" : ":8900",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot3.hdb",
  "raftstatefilename" : "Persister/raftstate3.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}
```

客户端进程A修改配置文件如下：
```json
{
  "client_address": ["localhost:8900","localhost:8901","localhost:8902"],
  "maxreries" : 100
}
```

当然你需要手速快一点，因为是本机运行，所以需要多次修改服务器配置文件，三个服务器进程执行如下指令：
```shell
go run server_base.go
```

其中一个进程会显示如下消息：
```shell
2020/11/01 16:00:13 INFO : [1145131042360542] become new leader! 
2020/11/01 16:00:13 INFO : The server is not connected to other servers in the cluster.
2020/11/01 16:00:13 INFO : The server is not connected to other servers in the cluster.
```
当" The server is not connected to other servers in the cluster."不再打印的时候服务部署成功。手速越慢，打印的时间越长。

相应的，其他两个客户端会打印:
```shell
2020/11/01 16:00:13 INFO : [1145151409896414] become new follower!
```

此时在客户端进程执行如下操作之一:
```shell
go run client_base.go
go run lock_base.go
go run lock_expand.go
```

随后就可以看到执行成功的日志。

## 6.测试
目前的测试代码分为几个部分：
1. 对于BaseMap，Sync.Map，ConcurrentMap的性能对比，解释了最终选择ChubbyGoMap的原因，在最优的性能下提供最大的灵活性。
2. 三个测试文件的定义。
### 6.1 MapPerformanceTest
测试代码位于：
```shell
ChubbyGo/MapPerformanceTest/test_test.go
```
执行如下指令可以执行测试文件：
```shell
cd MapPerformanceTest 
go test -v -run=^$ -bench . -benchmem
```

具体的内容可查看：
```shell
ChubbyGo/MapPerformanceTest/README.md
```
### 功能测试
1. **client_base.go**: 并发的执行get/set,可以自行配置并发的协程数和选择Get或者FastGet。
2. **lock_base.go**: 执行锁的全部基础操作。
3. **lock_expand.go**: 并发的请求读锁和写锁。

## 7.Thanks
感谢宋辉老师提出的宝贵意见。

感谢李浩提供了一种更优的服务器启动时间间隔处理函数。

感谢李怡提供了ChubbyGo Logo第一版与第二版的设计，由此才产生了Go这个可爱的小家伙儿。

## 7.其他
这是我对于ChubbyGo的一些想法与安全性论证：
1. [《Using ChubbyGo ！Join in ChubbyGo！》](https://blog.csdn.net/weixin_43705457/article/details/109446869)
2. 

## 8.参考
1. 《The Chubby lock service for loosely-coupled distributed systems》
2. 《In Search of an Understandable Consensus Algorithm(Extended Version)》
3. 《ZooKeeper: Wait-free coordination for Internet-scale systems》
