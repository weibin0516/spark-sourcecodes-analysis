对于一些比较简单的application，我们可以在IDEA编码并直接以local的方式在IDEA运行。有两种方法：

##一
在创建SparkContext对象时，指定以local方式执行，如下

```
val sc = new SparkContext("local", "app name")
```

##二
修改执行配置，如下

![](http://upload-images.jianshu.io/upload_images/204749-4e73c1b0e636edc0?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

---

当然，运行的前提是将必要的jar包在Libraries中配置好，如图:
![](http://upload-images.jianshu.io/upload_images/204749-de55f3861c012d68?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

##三
如果你还想直接在IDEA中调试spark源码，按f7进入.class后，点击
![](http://upload-images.jianshu.io/upload_images/204749-0542504740711a8c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

选择你在官网下载的与你的jar包版本一致的源码

![](http://upload-images.jianshu.io/upload_images/204749-72d5bb9d501d22e1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

之后，你就可以任意debug了～

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
