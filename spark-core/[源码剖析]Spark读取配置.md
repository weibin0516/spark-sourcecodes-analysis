# Spark读取配置

我们知道，有一些配置可以在多个地方配置。以配置executor的memory为例，有以下三种方式：
1. spark-submit的```--executor-memory```选项
2. spark-defaults.conf的```spark.executor.memory```配置
3. spark-env.sh的```SPARK_EXECUTOR_MEMORY```配置

同一个配置可以在多处设置，这显然会造成迷惑，不知道spark为什么到现在还保留这样的逻辑。
如果我分别在这三处对executor的memory设置了不同的值，最终在Application中生效的是哪个？

处理这一问题的类是```SparkSubmitArguments```。在其构造函数中就完成了从 『spark-submit --选项』、『spark-defaults.conf』、『spark-env.sh』中读取配置，并根据策略决定使用哪个配置。下面分几步来分析这个重要的构造函数。

##Step0：读取spark-env.sh配置并写入环境变量中
SparkSubmitArguments的参数列表包含一个```env: Map[String, String] = sys.env```参数。该参数包含一些系统环境变量的值和从spark-env.sh中读取的配置值，如图是我一个demo中env值的部分截图


![](http://upload-images.jianshu.io/upload_images/204749-73203a9b36f86c6a.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这一步之所以叫做Step0，是因为env的值在构造SparkSubmitArguments对象之前就确认，即```spark-env.sh```在构造SparkSubmitArguments对象前就读取并将配置存入env中。


##Step1：创建各配置成员并赋空值
这一步比较简单，定义了所有要从『spark-submit --选项』、『spark-defaults.conf』、『spark-env.sh』中读取的配置，并赋空值。下面的代码展示了其中一部分 ：

```
var master: String = null
var deployMode: String = null
var executorMemory: String = null
var executorCores: String = null
var totalExecutorCores: String = null
var propertiesFile: String = null
var driverMemory: String = null
var driverExtraClassPath: String = null
var driverExtraLibraryPath: String = null
var driverExtraJavaOptions: String = null
var queue: String = null
var numExecutors: String = null
var files: String = null
var archives: String = null
var mainClass: String = null
var primaryResource: String = null
var name: String = null
var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
var jars: String = null
var packages: String = null
var repositories: String = null
var ivyRepoPath: String = null
var packagesExclusions: String = null
var verbose: Boolean = false

...
```

##Step2：调用父类parse方法解析 spark-submit --选项

```
  try {
    parse(args.toList)
  } catch {
    case e: IllegalArgumentException => SparkSubmit.printErrorAndExit(e.getMessage())
  }
```

这里调用父类的```SparkSubmitOptionParser#parse(List<String> args)```。parse函数查找args中设置的--选项和值并解析为name和value，如```--master yarn-client```会被解析为值为```--master```的name和值为```yarn-client```的value。这之后调用```SparkSubmitArguments#handle(MASTER, "yarn-client")```进行处理。

来看看handle函数干了什么：

```

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value

      case MASTER =>
        master = value

      case CLASS =>
        mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      ...
      
      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

```

这个函数也很简单，根据参数opt及value，设置各个成员的值。接上例，parse中调用```handle("--master", "yarn-client")```后，在handle函数中，master成员将被赋值为```yarn-client```。

注意，case MASTER中的MASTER的值在```SparkSubmitOptionParser```定义为```--master```，MASTER与其他值定义如下：

```
protected final String MASTER = "--master";

protected final String CLASS = "--class";
protected final String CONF = "--conf";
protected final String DEPLOY_MODE = "--deploy-mode";
protected final String DRIVER_CLASS_PATH = "--driver-class-path";
protected final String DRIVER_CORES = "--driver-cores";
protected final String DRIVER_JAVA_OPTIONS =  "--driver-java-options";
protected final String DRIVER_LIBRARY_PATH = "--driver-library-path";
protected final String DRIVER_MEMORY = "--driver-memory";
protected final String EXECUTOR_MEMORY = "--executor-memory";
protected final String FILES = "--files";
protected final String JARS = "--jars";
protected final String KILL_SUBMISSION = "--kill";
protected final String NAME = "--name";
protected final String PACKAGES = "--packages";
protected final String PACKAGES_EXCLUDE = "--exclude-packages";
protected final String PROPERTIES_FILE = "--properties-file";
protected final String PROXY_USER = "--proxy-user";
protected final String PY_FILES = "--py-files";
protected final String REPOSITORIES = "--repositories";
protected final String STATUS = "--status";
protected final String TOTAL_EXECUTOR_CORES = "--total-executor-cores";

...
```

总结来说，parse函数解析了spark-submit中的--选项，并根据解析出的name和value给SparkSubmitArguments的各个成员（例如master、deployMode、executorMemory等）设置值。

##Step3：mergeDefaultSparkProperties加载spark-defaults.conf中配置

Step3读取spark-defaults.conf中的配置文件并存入sparkProperties中，sparkProperties将在下一步中发挥作用

```
//< 保存从spark-defaults.conf读取的配置
val sparkProperties: HashMap[String, String] = new HashMap[String, String]()

//< 获取配置文件路径，若在spark-env.sh中设置SPARK_CONF_DIR，则以该值为准；否则为 $SPARK_HOME/conf/spark-defaults.conf
def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
  env.get("SPARK_CONF_DIR")
    .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
    .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
    .filter(_.isFile)
    .map(_.getAbsolutePath)
    .orNull
}

//< 读取spark-defaults.conf配置并存入sparkProperties中
private def mergeDefaultSparkProperties(): Unit = {
  // Use common defaults file, if not specified by user
  propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
  // Honor --conf before the defaults file
  defaultSparkProperties.foreach { case (k, v) =>
    if (!sparkProperties.contains(k)) {
      sparkProperties(k) = v
    }
  }
}
```

##Step4：loadEnvironmentArguments确认每个配置成员最终值
先来看看代码（由于篇幅太长，省略了一部分）

```
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    
    ...

    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    action = Option(action).getOrElse(SUBMIT)
  }
```

我们单独以确定master值的那部分代码来说明，相关代码如下

```
master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull

// Global defaults. These should be keep to minimum to avoid confusing behavior.
master = Option(master).getOrElse("local[*]")
```

确定master的值的步骤如下：
1. **Option(master)**：若master值不为null，则以master为准；否则进入2。若master不为空，从上文的分析我们可以知道是从解析spark-submit --master选项得到的值
2. **.orElse(sparkProperties.get("spark.master"))**：若sparkProperties.get("spark.master")范围非null则以该返回值为准；否则进入3。从Step3中可以知道sparkProperties中的值都是从spark-defaults.conf中读取
3. **.orElse(env.get("MASTER"))**：若env.get("MASTER")返回非null，则以该返回值为准；否则进入4。env中的值从spark-env.sh读取而来
4. 若以上三处均为设置master，则取默认值local[*]

查看其余配置成员的值的决定过程也和master一致，稍有不同的是并不是所有配置都能在spark-defaults.conf、spark-env.sh和spark-submit选项中设置。但优先级还是一致的。

由此，我们可以得出结论，对于spark配置。若一个配置在多处设置，则优先级如下：
***spark-submit --选项 > spark-defaults.conf配置 > spark-env.sh配置 > 默认值***

最后，附上流程图


![](http://upload-images.jianshu.io/upload_images/204749-f69827e92149a1a2.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

---

欢迎关注我的微信公众号：FunnyBigData

![FunnyBigData](http://upload-images.jianshu.io/upload_images/204749-2f217e5d38fc1bcb.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
