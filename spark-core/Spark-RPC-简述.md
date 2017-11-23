Spark 中的消息通信主要涉及 RpcEnv、RpcEndpoint 及 RpcEndpointRef 几个类，下面进行简单介绍
## RpcEnv、RpcEndpoint 及 RpcEndpointRef
RPCEndpoints 定义了如何处理消息（即，使用哪个函数来处理指定消息）,在通过 name 完成注册后，RpcEndpoint 就一直存放在 RpcEnv 中。RpcEndpoint 的生命周期按顺序是 ```onStart```，```receive``` 及 ```onStop```，```receive``` 可以被同时调用，如果希望 ```receive``` 是线程安全的，可以使用 ```ThreadSafeRpcEndpoint```

```RpcEndpointRef``` 是 RpcEnv 中的 RpcEndpoint 的引用，是一个序列化的实体以便于通过网络传送或保存以供之后使用。一个 RpcEndpointRef 有一个地址和名字。可以调用 ```RpcEndpointRef``` 的 ```send``` 方法发送异步的单向的消息给对应的 RpcEndpoint

RpcEnv 管理各个 RpcEndpoint 并将发送自 RpcEndpointRef 或远程节点的消息分发给对应的 RpcEndpoint。对于 RpcEnv 没有 catch 到的异常，会通过 ```RpcCallContext.sendFailure``` 将该异常发回给消息发送者或记日志

![](http://upload-images.jianshu.io/upload_images/204749-d89590e3a1c84c0d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## RpcEnvFactory
RpcEnvFactory 是构造 RpcEnv 的工厂类，调用其 ```create(config: RpcEnvConfig): RpcEnv``` 会 new 一个 RpcEnv 实例并返回。

Spark 中实现了两种 RpcEnvFactory：

* ```org.apache.spark.rpc.netty.NettyRpcEnvFactory``` 使用 ```netty```
* ```org.apache.spark.rpc.akka.AkkaRpcEnvFactory``` 使用 ```akka```

其中在 Spark 2.0 已经没有了 ```AkkaRpcEnvFactory```，仅保留了 ```NettyRpcEnvFactory```。在 Spark 1.6 中可以通过设置 ```spark.rpc``` 值为 ```netty``` （默认）来使用 ```NettyRpcEnvFactory``` 或设置为 ```akka``` 来使用 ```AkkaRpcEnvFactory```，例如：

```
$ ./bin/spark-shell --conf spark.rpc=netty
$ ./bin/spark-shell --conf spark.rpc=akka
```

## RpcAddress 与 RpcEndpointAddress
RpcAddress 是一个 RpcEnv 的逻辑地址，包含 hostname 和端口，RpcAddress 像 Spark URL 一样编码，比如：```spark://host:port```。RpcEndpointAddress 是向一个 RpcEnv 注册的 RpcEndpoint 的逻辑地址，包含 RpcAddress 及名字，格式如：```spark://[name]@[rpcAddress.host]:[rpcAddress.port]```

## 参考
* https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-rpc.html

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
