**[English](README.md)** | 简体中文

# Cloud Shuffle Service

[![GitHub license](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/bytedance/ByteX/blob/master/LICENSE)

Cloud Shuffle Service(CSS) 是一个通用的远程shuffle解决方案。其支持当前主流计算引擎，如Spark/Flink/MapReduce等，
并为这些主流的计算框架提供相比原生方案更可靠的、性能更高的、更弹性的数据shuffle能力。
计算框架将shuffle数据推送到CSS集群并存放在磁盘或者HDFS中，
之后当需要读取数据时，再通过CSS集群的接口拉取相关的shuffle数据。


![CSS Architecture](docs/img/css-arch.png)
- CSS Worker 

  负责把来自Map Task发送的数据存储到内存中并最终异步的把数据刷写到文件系统中。当Reduce Task开始时可以从Worker拉取之前存储的数据。

- CSS Master
  
  负责协调application的整个shuffle过程，需要被整合进application的运行过程中，例如跑在Spark的driver中。
  CSS Master会从ZooKeeper中获取worker的列表并且分配合适的worker给application提供shuffle服务，并且跟进所有Map Task完成的进度，
  当Map Task运行完成的时候会通知所有worker把所有缓存中的数据持久化到磁盘并关闭文件。

- CSS Client

  当运行Map或者reduce task时，负责从所有worker推送或者拉取shuffle数据

## 编译 CSS

### mvn build

CSS 使用 [Apache Maven](https://maven.apache.org/) 作为编译工具。使用Maven编译CSS需要使用Java 8，和Scala 2.12或Scala 2.11

```bash
mvn -DskipTests clean package
```
### 编译可执行文件
使用项目根目录下的 ```./build.sh``` 来编译得到可执行文件
```bash
./build.sh
```
执行之后会生成一个tgz包，可以将其拷贝到想要部署 CSS 服务的节点上
```
css-1.0.0-bin
├── LICENSE
├── README.md
├── client
├── conf
├── docs
├── lib  // CSS cluster lib
└── sbin
```

## 部署 CSS 集群
CSS 提供两种部署模式，分别是独立部署模式和Zookeeper模式。其中独立部署模式当前仅支持测试场景，在生产场景下建议使用zookeeper模式。

1. 把按照之前步骤编译好的tgz包发送到集群中的每个节点上
2. 解压该tgz包到某个目录，并设置该目录路径作为环境变量CSS_HOME的值，集群默认配置文件、metrics配置文件、worker列表文件都放在```$CSS_HOME/conf```目录下
3. 修改 ```$CSS_HOME/sbin/css-config.sh```
   ```
   # standalone mode
   CSS_MASTER_HOST=<HOST_IP>
   MASTER_JAVA_OPTS="-Xmx8192m"
   WORKER_JAVA_OPTS="-Xmx8192m -XX:MaxDirectMemorySize=100000m"
    
   # zookeeper mode
   WORKER_JAVA_OPTS="-Xmx8192m -XX:MaxDirectMemorySize=100000m"
   ```
4. 修改 ```$CSS_HOME/conf/css-defaults.conf```
   ```
   css.cluster.name = <css cluster name>
 
   # standalone(for testing) or zookeeper(for production)
   css.worker.registry.type = zookeeper
   # only for zookeeper mode
   css.zookeeper.address = <ip1>:<port1>,<ip2>:<port2>,<ip3>:<port3>
   
   # css worker common conf
   css.flush.queue.capacity = 4096
   css.flush.buffer.size = 128k
   css.network.timeout = 600s
   css.epoch.rotate.threshold = 1g
   css.push.io.numConnectionsPerPeer = 8
   css.push.io.threads = 128
   css.replicate.threads = 128
   css.fetch.io.threads = 64
   css.fetch.chunk.size = 4m
   css.shuffle.server.chunkFetchHandlerThreadsPercent = 400
   
   # hdfs storage
   css.hdfsFlusher.base.dir = hdfs://xxx
   css.hdfsFlusher.num = -1
   css.hdfsFlusher.replica = 2    
 
   # local disk storage
   css.diskFlusher.base.dirs = /data00/css,/data01/css
   css.disk.dir.num.min = 1
   ```
5. 自定义 metrics 配置文件和 worker 列表文件
   ```
   $CSS_HOME/conf/css-metrics.properties
   $CSS_HOME/conf/workers
   ```
6. 把上述所有配置文件发送到集群中所有的节点上
7. 执行如下命令启动所有的Css Workers，所有的worker所在的节点需要该执行该命令的机器能够通过SSH正常连接
   ```
   # standalone mode
   cd $CSS_HOME;bash ./sbin/start-all.sh
   # zookeeper mode
   cd $CSS_HOME;bash ./sbin/start-workers.sh
   ```

## Spark使用CSS
1. 拷贝 ```$CSS_HOME/client/spark-${version}/*.jar``` 到 ```$SPARK_HOME/jars/``` 
2. 启动 spark 时添加如下参数
   ```
   # standalone mode
   --conf spark.css.cluster.name=<css cluster name> \
   --conf spark.css.master.address=css://<masterIp>:<masterPort>\
   --conf spark.shuffle.manager=org.apache.spark.shuffle.css.CssShuffleManager\
 
   # zookeeper mode
   --conf spark.css.cluster.name=<css cluster name> \
   --conf spark.css.zookeeper.address="<ip1>:<port1>,<ip2>:<port2>,<ip3>:<port3>" \
   --conf spark.shuffle.manager=org.apache.spark.shuffle.css.CssShuffleManager\
   ```

## 支持 Spark Adaptive Query Execution
CSS 支持 Spark AQE 所有特性。对于SkewJoin的支持，需要使用以下文件更新Spark的源码并重新编译Spark。
```
./patch/spark-3.0-aqe-skewjoin.patch
```

## 配置项
### CSS 服务端配置
All detailed configuration can be found in CssConf class.

| Property Name             | Default    | Meaning                                                                                                                           |
|---------------------------|------------|-----------------------------------------------------------------------------------------------------------------------------------|
| css.cluster.name          | -          | The cluster name for the CSS cluster.                                                                                             |
| css.worker.registry.type  | standalone | The worker registry type (e.g. standalone, zookeeper). This will also specify if CSS will run under Standalone or zookeeper mode. |
| css.zookeeper.address     | -          | (For zookeeper mode) The CSS zookeeper address.                                                                                   |
| css.push.io.threads       | 32         | The CSS Threads for netty push data io.                                                                                           |
| css.fetch.io.threads      | 32         | The CSS Threads for netty fetch data io.                                                                                          |
| css.commit.threads        | 128        | The CSS Threads for stage end to close partition file.                                                                            |
| css.diskFlusher.base.dirs | /tmp/css   | The CSS Disk Base dirs (e.g. /data00/css,/data01/css).                                                                            |
| css.hdfsFlusher.base.dir  | -          | The CSS HDFS Base dir (e.g. hdfs://xxx).                                                                                          |

### CSS 客户端配置

| Property Name                             | Default | Meaning                                                                                                                                                                                                                           |
|-------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| css.max.allocate.worker                   | 1000    | The Maximum number of workers requested for shuffling.                                                                                                                                                                            |
| css.worker.allocate.extraRatio            | 1.5     | The application can allocate additional workers controlled by this extra ratio, the final number will be calculated with Min(Max(2, targetWorker), MaxAllocateWorker).                                                            |
| css.backpressure.enabled                  | true    | The back pressure control, when enabled, it will use Gradient2Limit to control push data rate, otherwise use FixedLimit.                                                                                                          |
| css.fixRateLimit.threshold                | 64      | Fixed Rate for the back pressure control.                                                                                                                                                                                         |
| css.data.io.threads                       | 8       | The Maximum client side data sending for netty thread.                                                                                                                                                                            |
| css.maxPartitionsPerGroup                 | 100     | The Maximum number of partitions per group, each data push will send one group at a time.                                                                                                                                         |
| css.partitionGroup.push.buffer.size       | 4m      | The Maximum buffer size sent per each data push, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).                                                                         |
| css.client.mapper.end.timeout             | 600s    | The Maximum timeout to wait for all data to be sent before mapTask ends.                                                                                                                                                          |
| css.stage.end.timeout                     | 600s    | The Maximum timeout to wait for all partition files to close.                                                                                                                                                                     |
| css.sortPush.spill.record.threshold       | 1000000 | The Maximum records for sending data.                                                                                                                                                                                             |
| css.sortPush.spill.size.threshold         | 256m    | The Maximum size for sending data, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).                                                                                       |
| css.shuffle.mode                          | DISK    | Choose which storage mode to use (e.g. DISK, HDFS).                                                                                                                                                                               |
| css.epoch.rotate.threshold                | 1g      | The file auto rotate switch threshold size for new files, in the same format as JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g. 512m, 2g).                                                                |
| css.client.failed.batch.blacklist.enabled | true    | When MapTask encounters onFailure, the current reduceId-epochId-mapId-mapAttemptId-batchId will be recorded into the blacklist. In AE skewjoin mode, this switch must be turned on, otherwise there will be correctness problems. |
| css.compression.codec                     | lz4     | It is recommended to use zstd compression mode. Compared with lz4, it can improve the compression ratio by 30%, and only consume an additional 8% of performance.                                                                 |

## 贡献协议

请点击[Contributing](CONTRIBUTING.md)查看更多细节.

## 行为准则

请点击[Code of Conduct](CODE_OF_CONDUCT.md)查看更多细节.

## 安全漏洞

如果你在此项目中发现了一个潜在的安全问题，请联系[字节跳动安全中心](https://security.bytedance.com/src) 或发送邮件到[漏洞汇报](mailto:sec@bytedance.com).

请**不要**创建公开的Github issue.

## 开源协议

本项目采用[Apache-2.0 License](LICENSE)协议.