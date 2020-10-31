# 简介
对于线程安全的hashmap的选取是性能提升的一个重点，摆在面前的有三种选择，BaseMap，sync.map以及concurrentMap。

 1. **BaseMap**：map中只有一把锁，所有操作都经过这把锁。
 2. **sync.map**：Golang提供的一个线程安全的哈希map，采用无锁实现。
 3. **concurrentMap**：每一个哈希桶加一把锁，最大程度并发。

一般情况直接使用标准库提供的就不会有太大问题，但在简单的查阅了sync.map的实现后我认为此种方法在多写的情况下并不高效，因为涉及到很多次的拷贝，为了更优的性能，我测试了这三种方案在不同情况下的性能，最终发现BaseMap效率在任何情况下表现都不突出，而其他两种方法各有优略，最终我选择的版本是客户可以根据不同的应用条件在配置文件中动态的配置选择sync.map还是concurrentMap，为了更优雅的代码与更少的空间消耗，引入反射动态区分这两种类型，最终带来了效率上一定的损耗，但增加了灵活性，我称之为ChubbyGoMap。


以下是BaseMap, sync.map, concurrentMap, 以及ChubbyGoMap的两种类型五种类型分别执行Put，Get，并发插入读取混合，删除四种操作的性能对比，以此为客户提供不同场景下最佳的选择：

测试代码位于/ChubbyGo/MapPerformanceTest/test_test.go：

执行以下命令可以执行测试：

```go
go test -v -run=^$ -bench . -benchmem
```

说明：

 1. ns/op : 每次执行平均花费时间;
 2. b/op: 每次执行堆上分配内存总数;
 3. allocs/op: 每次执行堆上分配内存次数;

## Put
|  | ns/op | b/op| allocs/op|
|--|--|--|--|--|
|BaseMap  | 2505 | 205| 10
|ConcurrentMap|1257| 239|10
|SyncMap| 4546|500|32
|GoConcurrentMap|1838|423|20
|GoSyncMap|5096|662|42
