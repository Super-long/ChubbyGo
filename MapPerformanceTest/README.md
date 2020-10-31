[![License](http://www.apache.org/licenses/LICENSE-2.0)](LICENSE)
# Introduction
对于线程安全的hashmap的选取是性能提升的一个重点，摆在面前的有三种选择，BaseMap，sync.map以及concurrentMap。

 1. **BaseMap**：map中只有一把锁，所有操作都经过这把锁。
 2. **sync.map**：Golang提供的一个线程安全的哈希map，采用无锁实现。
 3. **concurrentMap**：每一个哈希桶加一把锁，最大程度并发。

一般情况直接使用标准库提供的就不会有太大问题，但在简单的查阅了sync.map的实现后我认为此种方法在多写的情况下并不高效，因为涉及到很多次的拷贝，为了更优的性能，我测试了这三种方案在不同情况下的表现，最终发现BaseMap效率在任何情况下表现都不突出，而其他两种方法各有优略，最终我选择的版本是客户可以根据不同的应用条件在配置文件中动态的配置选择sync.map还是concurrentMap，为了更优雅的代码与更少的空间消耗，引入反射动态区分这两种类型，最终带来了效率上一定的损耗，但增加了灵活性，我称之为ChubbyGoMap。


以下是BaseMap, sync.map, concurrentMap, 以及ChubbyGoMap的两种类型五种类型分别执行Put，Get，插入读取混合，删除四种操作的性能对比，以此为客户提供不同场景下最佳的选择：

测试代码位于/ChubbyGo/MapPerformanceTest/test_test.go：

执行以下命令可以执行测试：

```go
go test -v -run=^$ -bench . -benchmem
```

说明：

 1. ns/op : 每次执行平均花费时间;
 2. b/op: 每次执行堆上分配内存总数;
 3. allocs/op: 每次执行堆上分配内存次数;
 
 所有数据为五次测试的平均值。

## Put

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 2505 | 205| 10
|ConcurrentMap|1257| 239|10
|SyncMap| 4546|500|32
|GoConcurrentMap|1838|423|20
|GoSyncMap|5096|662|42

## Get

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 1654 | 0| 0
|ConcurrentMap|663| 77|10
|SyncMap| 533|4|1
|GoConcurrentMap|1048|76|10
|GoSyncMap|758|77|10

## Get&Put

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 4565 | 266| 20
|ConcurrentMap|2926| 311|20
|SyncMap| 7149|836|42
|GoConcurrentMap|3021|477|30
|GoSyncMap|7275|938|52

## Delete

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 4346 | 139| 19
|ConcurrentMap|1719| 152|19
|SyncMap| 7113|1012|48
|GoConcurrentMap|2634|305|29
|GoSyncMap|7272|1296|61

## summary
由以上数据可以看出sync.map涉及Put的操作中效率都比较低，而在Get较多的时候效率极高，其实这也很好想象，因为此时sync.map中的读操作是完全无锁的，有兴趣的朋友可以看源码了解。

因为ChubbyGo中的FastGet操作是不操作ChubbyGoMap的，再加上如上的性能测试，我们可以得出结论：当使用ChubbyGo为读多写少时选择SyncMap策略，其他时候均使用ConcurrentMap策略，可在json配置文件中进行配置。