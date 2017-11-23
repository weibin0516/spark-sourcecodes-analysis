> Task 的执行流程相关内容在一年多以前的文章 [Task的调度与执行源码剖析](http://www.jianshu.com/p/9a059ace2f3a) 中已经介绍了很多，但那篇文章内容过长、条理不够清楚并且版本过于久远（本次针对2.0），这里趁分析 executor 模块的机会再写一写

Task 的执行流程分以下四篇进行介绍：

1. [为 task 分配 executor](http://www.jianshu.com/p/17a61ff4d65c)
2. [创建、分发 Task](http://www.jianshu.com/p/08c66cbc31d6)
3. [执行 Task](http://www.jianshu.com/p/8bb456cb7c77)
4. [task 结果的处理](http://www.jianshu.com/p/c204735a6bc3)

参考：《Spark技术内幕》

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
