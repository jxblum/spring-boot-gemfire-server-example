# Spring Boot GemFire Server Example

The *Spring Boot GemFire Server Example* project demonstrates how to configure and bootstrap an embedded 
*Pivotal GemFire* Server (i.e. peer cache member) in a *Spring Boot* application JVM process.

This *GemFire Server* is configured using *Spring (Data GemFire)* Java configuration meta-data and assumes the user 
wants to connect this server to an existing *GemFire Cluster*.  The supposedly existing *GemFire Cluster* is defined by
a *GemFire Locator* running on the `localhost`, listening on port `10334` (the default *Locator* port)
for member join requests.

The *Spring* Java configuration meta-data allows for certain System properties to be configured on the command-line
to adjust, among other things, the `log-level` or the `cluster` to which this server will connect.

More details on how to run this example can be seen below.

### Requirements

* *Java Development Kit* (JDK) 1.8.0+ (e.g. 1.8.0_72).
* *Gradle* 2.2+

### Dependencies

This project primarily depends on *Spring Boot* `1.3.5.RELEASE`, *Spring Data GemFire* `1.8.2.RELEASE` 
and *Pivotal GemFire* `8.2.0`
 
You can download distributions of the *JDK* [here](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

This project uses the [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) so no explicit 
*Gradle* installation is necessary.

### Build

To build the *spring-boot-gemfire-server* project, run the `gradlew` shell script at the command-line

```
$ gradlew clean build install
```

### Run

To run this example, you must first start a `GemFire Locator` using *Gfsh* (the *GemFire Shell* tool), like so...

```
$ echo $GEMFIRE
/Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux

$ gfsh

    _________________________     __
   / _____/ ______/ ______/ /____/ /
  / /  __/ /___  /_____  / _____  / 
 / /__/ / ____/  _____/ / /    / /  
/______/_/      /______/_/    /_/    v8.2.0

Monitor and Manage GemFire

gfsh>start locator --name=GemFireLocator --log-level=config
Starting a GemFire Locator in /Users/jblum/pivdev/lab/GemFireLocator...
.............................
Locator in /Users/jblum/pivdev/lab/GemFireLocator on 10.99.199.9[10334] as GemFireLocator is currently online.
Process ID: 81551
Uptime: 15 seconds
GemFire Version: 8.2.0
Java Version: 1.8.0_72
Log File: /Users/jblum/pivdev/lab/GemFireLocator/GemFireLocator.log
JVM Arguments: -Dgemfire.enable-cluster-configuration=true -Dgemfire.load-cluster-configuration-from-dir=false -Dgemfire.log-level=config -Dgemfire.launcher.registerSignalHandlers=true -Djava.awt.headless=true -Dsun.rmi.dgc.server.gcInterval=9223372036854775806
Class-Path: /Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/gemfire.jar:/Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/locator-dependencies.jar

Successfully connected to: [host=10.99.199.9, port=1099]

Cluster configuration service is up and running.

```

Once the *GemFire Locator* (also a *GemFire Manager*) is started, you can use the `list members` 
and `describe member` commands to find out more details...
  
```
gfsh>list members
Member Count : 1
Coordinator  : GemFireLocator (jblum-mbpro-2(GemFireLocator:81551:locator)<v0>:28961)

     Name      | Id
-------------- | -----------------------------------------------------
GemFireLocator | jblum-mbpro-2(GemFireLocator:81551:locator)<v0>:28961


gfsh>describe member --name=GemFireLocator
Name        : GemFireLocator
Id          : jblum-mbpro-2(GemFireLocator:81551:locator)<v0>:28961
Host        : 10.99.199.9
Regions     : 
PID         : 81551
Groups      : 
Used Heap   : 60M
Max Heap    : 3641M
Working Dir : /Users/jblum/pivdev/lab/GemFireLocator
Log file    : /Users/jblum/pivdev/lab/GemFireLocator/GemFireLocator.log
Locators    : 10.99.199.9[10334]```
```

You can continue to start and add additional *GemFire Locators* or even other *GemFire Servers* to the cluster.

However, you must be careful to configure network ports accordingly, especially if you configure and start more than
one *Locator*, *Manager* or even other *GemFire Servers* that will also serve as *Cache Servers* to 
cache clients, since all newly added members will attempt to bind to the default port for the aforementioned services 
respectively (Location, Management and Cache Serving), which will result in `BindExceptions` with port already in use.
  
At this point, you may start the *Spring Boot*-based *GemFire Server* application.  If you changed 
the *GemFire Locator's* port on startup to anything other than the default port (`10334`), then you must specify 
the *Locator`s* port when starting the *Spring Boot GemFire Server* in order for the server to find the *Locator*
and join the cluster (formed in *Gfsh*).

For instance, if you started the *GemFire Locator* like so...

```
gfsh>start locator --name=Example --port=11235
```

Then you must also specify the appropriate application property when starting the *Spring Boot GemFire Server* 
application.  

### Issues

For any issues or feedback on this project, please file a [GitHub Issue](https://github.com/jxblum/spring-boot-gemfire-server/issues).  

### Contributions

I welcome any contributions through [Pull Requests](https://github.com/jxblum/spring-boot-gemfire-server-/pulls).
If you have an idea or improvement, please share.  No CLA necessary.

**Thanks**
