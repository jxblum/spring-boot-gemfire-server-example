# Spring Boot GemFire Server Example

The *Spring Boot GemFire Server Example* project demonstrates how to configure and bootstrap
an embedded *Pivotal GemFire* or *Apache Geode* Server (i.e. a peer cache member/data node)
in a *Spring Boot* application JVM process.

The *GemFire Server* is configured using *Spring Data GemFire's*
[new Annotation-based configuration model](https://docs.spring.io/spring-data/geode/docs/current/reference/html/#bootstrap-annotation-config)
(not XML and not plain Java config) and assumes (OOTB) the user wants to connect this server to an existing *GemFire Cluster*.
It is also assumed that this "existing" *GemFire Cluster* is defined by a *GemFire Locator* running on `localhost`,
listening on port `10334` (the default *Locator* port) for member join requests.

The *Spring* Annotation-based configuration meta-data allows for certain application properties
to be configured in order to adjust, among other things, *GemFire's* `log-level` or the `cluster`
to which this server will connect, and so on.

More information on how to run this example is detailed below.

### Requirements

* *Java Development Kit* (JDK) 1.8.0+ (e.g. 1.8.0_152).
* *Gradle* 4.6.

### Dependencies

This project runs on *Spring Boot* `2.0.0.RELEASE` with *Spring Data GemFire* `2.0.5.RELEASE`
and *Pivotal GemFire* `9.1.1`, the latest releases for each of these technologies at the time
of this writing.

Use the `spring-boot-15x-pivotal-gemfire-82x` branch to run this example on *Spring Boot*
`1.5.10.RELEASE`, with *Spring Data GemFire* `1.9.10.RELEASE` and Pivotal GemFire `8.2.8`
dependencies.

> NOTE: versions may have been updated since this writing.

You can download distributions of the *JDK* [here](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

This project uses the [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html)
so no explicit *Gradle* installation is necessary.

### Build

To build the *spring-boot-gemfire-server-example* project, run the `gradlew` shell script
at the command-line:

```
$ gradlew clean build install
```

### Run

> **WARNING: This section still needs to be updated for _Spring Boot_ `2.0.0.RELEASE`,
_Spring Data GemFire_ `2.0.5.RELEASE` and _Pivotal GemFire_ `9.1.1`.**

To run this example, you will need a full installation of Pivotal GemFire installed on your system
to use the GemFire Shell (i.e. _Gfsh_) tool from the command-line.

For this example, I downloaded and installed *Pivotal GemFire* `9.1.1` from
[Pivotal.io](https://pivotal.io/pivotal-gemfire).

See the *Pivotal GemFire* installation instructions [here](http://gemfire91.docs.pivotal.io/gemfire/getting_started/installation/install_intro.html).

```
$ echo $GEMFIRE
/Users/jblum/Downloads/Pivotal/GemStone/Products/PivotalGemFire/Pivotal_GemFire_829_b16_Linux

$ gfsh
    _________________________     __
   / _____/ ______/ ______/ /____/ /
  / /  __/ /___  /_____  / _____  /
 / /__/ / ____/  _____/ / /    / /
/______/_/      /______/_/    /_/    v8.2.9

Monitor and Manage GemFire
gfsh>start locator --name=GemFireLocator --log-level=config
Starting a GemFire Locator in /Users/jblum/pivdev/lab/GemFireLocator...
.............................
Locator in /Users/jblum/pivdev/lab/GemFireLocator on 10.0.0.121[10334] as GemFireLocator is currently online.
Process ID: 97948
Uptime: 15 seconds
GemFire Version: 8.2.9
Java Version: 1.8.0_152
Log File: /Users/jblum/pivdev/lab/GemFireLocator/GemFireLocator.log
JVM Arguments: -Dgemfire.enable-cluster-configuration=true -Dgemfire.load-cluster-configuration-from-dir=false -Dgemfire.log-level=config -Dgemfire.launcher.registerSignalHandlers=true -Djava.awt.headless=true -Dsun.rmi.dgc.server.gcInterval=9223372036854775806
Class-Path: /Users/jblum/Downloads/Pivotal/GemStone/Products/PivotalGemFire/Pivotal_GemFire_829_b16_Linux/lib/gemfire.jar:/Users/jblum/Downloads/Pivotal/GemStone/Products/PivotalGemFire/Pivotal_GemFire_829_b16_Linux/lib/locator-dependencies.jar

Successfully connected to: [host=10.0.0.121, port=1099]

Cluster configuration service is up and running.
```

Once the *GemFire Locator* (also a *GemFire Manager*) is started, you can use the `list members`
and `describe member` commands in *Gfsh* to find out more details about the cluster and it's members...

```
gfsh>list members
Member Count : 1
Coordinator  : GemFireLocator (10.0.0.121(GemFireLocator:97948:locator)<v0>:26480)

     Name      | Id
-------------- | --------------------------------------------------
GemFireLocator | 10.0.0.121(GemFireLocator:97948:locator)<v0>:26480

gfsh>describe member --name=GemFireLocator
Name        : GemFireLocator
Id          : 10.0.0.121(GemFireLocator:97948:locator)<v0>:26480
Host        : 10.0.0.121
Regions     :
PID         : 97948
Groups      :
Used Heap   : 82M
Max Heap    : 3641M
Working Dir : /Users/jblum/pivdev/lab/GemFireLocator
Log file    : /Users/jblum/pivdev/lab/GemFireLocator/GemFireLocator.log
Locators    : 10.0.0.121[10334]
```

You can continue to start and add additional *GemFire Locators* or *GemFire Servers* to the cluster.

However, you must be careful to configure network ports accordingly, especially if you configure and start
multiple *Locators*, *Servers* (especially severs that will also function and serve as a *CacheServer*s
listening for cache client connections) or even other *Managers*, since all newly added members will attempt
to bind to the default port for the aforementioned services respectively (Location, Management
and Cache Serving), which will lead to `BindExceptions` with ports already in use if not configured properly.

At this point, you may start the *Spring Boot GemFire Server* application.  If you changed the *GemFire Locator's* port
on startup to anything other than the default port (`10334`), then you must specify the *Locator`s* port when starting
the *Spring Boot GemFire Server* in order for the server to find the *Locator* and join to the cluster
(started and formed in *Gfsh*).

For instance, if you started the *GemFire Locator* like so...

```
gfsh>start locator --name=Example --port=11235
```

Then you must also set the appropriate [application property](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/resources/application.properties#L4)
in the _Spring Boot_ `application.properties` when starting the *Spring Boot GemFire Server* application.

Once configured, then the *Spring Boot GemFire Server* will be in a runnable state and should be able to connect to the cluster.
You may run the applicaton from inside your IDE of choice or from the command-line using either the *Spring Boot Gradle Plugin*:

```
$gradlew bootRun
```

Or, using the "executable" JAR file (`build/lib/spring-boot-gemfire-server-example-1.0.0.SNAPSHOT.jar`) created during
the *Gradle* build...

```
$java -jar build/libs/spring-boot-gemfire-server-example-1.0.0.SNAPSHOT.jar
```

This advantage when using the later approach is that you may adjust any of the [application.properties](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/resources/application.properties)
by using Java System properties from the command-line.

For example, you can adjust GemFire's log level using `gemfire.log-level` like so:

```
$java -Dspring.gemfire.log-level=warning -jar build/libs/spring-boot-gemfire-server-example-1.0.0.SNAPSHOT.jar
```

Once the *Spring Boot GemFire Server* successfully starts and connects to the cluster defined by
our *Gfsh* started *Locator* ("GemFireLocator"), then it will be possible to interact with this server
using *Gfsh*...

```
gfsh>list members
Member Count : 2
Coordinator  : GemFireLocator (10.0.0.121(GemFireLocator:97948:locator)<v0>:26480)

         Name           | Id
----------------------- | --------------------------------------------------
SpringBootGemFireServer | 10.0.0.121(SpringBootGemFireServer:97977)<v1>:1580
GemFireLocator          | 10.0.0.121(GemFireLocator:97948:locator)<v0>:26480

gfsh>describe member --name=SpringBootGemFireServer
Name        : SpringBootGemFireServer
Id          : 10.0.0.121(SpringBootGemFireServer:97977)<v1>:1580
Host        : 10.0.0.121
Regions     : Factorials
PID         : 97977
Groups      :
Used Heap   : 42M
Max Heap    : 3641M
Working Dir : /Users/jblum/pivdev/spring-data-examples-workspace/spring-boot-gemfire-server-example/build
Log file    : /Users/jblum/pivdev/spring-data-examples-workspace/spring-boot-gemfire-server-example/build
Locators    : localhost[10334],localhost[11334]

Cache Server Information
Server Bind              : localhost
Server Port              : 40404
Running                  : true
Client Connections       : 0
```

Note the details of interest:

1. First, you can see here that our *Spring Boot GemFire Server* is part of the cluster
defined by our *Locator* (i.e. "GemFireLocator") when using the `list members` command.
2. Next, our *Spring Boot*-configured *GemFire Server* was specifically named "`SpringBootGemFireServer`";
see [here](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L67)
and specifically [here](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L82-L84).
3. The *Spring* configuration also [defined and started a `CacheServer`](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L97-L113)
running on `localhost`, listening on the default port, `40404` (which is [configurable](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/resources/application.properties#L1-L3)).
4. Then, you will notice the "_Factorials_" `PARTITION` *Region* was [defined](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L115-L128).
5. Finally, the Spring configuration also [starts and embedded `Locator`](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L75-L77),
which makes it possible to form a small cluster without explicitly staring a `Locator` (e.g. "GemFireLocator") using _Gfsh_.

For more details on the "_Factorials_" *Region*, run `list regions` and `describe region` in *Gfsh*...

```
gfsh>list regions
List of regions
---------------
Factorials

gfsh>describe region --name=/Factorials
..........................................................
Name            : Factorials
Data Policy     : partition
Hosting Members : SpringBootGemFireServer

Non-Default Attributes Shared By Hosting Members

 Type  | Name | Value
------ | ---- | -----
Region | size | 0
```

To make this example more useful and interactive, the "`Factorials`" *Region* is exactly as the name implies,
it is a `Map` data structure (i.e. `PARTITION` *Region*) containing [factorials](https://en.wikipedia.org/wiki/Factorial).

The keys is a number used to compute the *factorial* and the value is the result of the *factorial* computation,
where *factorial* is defined as `factorial(n) = n!` (e.g. `factorial(4) = 4 * 3 * 2 * 1 = 24`).

The "`Factorials`" *Region* is not pre-loaded.  Instead, I have [defined](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L142-L171)
a simple *GemFire* `CacheLoader` that computes the *factorial* of a key (number), which was [added](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L123)
to the "`Factorials`" *Region* in the bean definition.

So, in order to compute the *factorial* of (just about) any number, you just need to request a key
using the _Gfsh_ `get` command:

```
fsh>get --region=/Factorials --key=1 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 1
Value Class : java.lang.Long
Value       : 1


gfsh>get --region=/Factorials --key=2 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 2
Value Class : java.lang.Long
Value       : 2


gfsh>get --region=/Factorials --key=3 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 3
Value Class : java.lang.Long
Value       : 6


gfsh>get --region=/Factorials --key=4 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 4
Value Class : java.lang.Long
Value       : 24


gfsh>get --region=/Factorials --key=5 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 5
Value Class : java.lang.Long
Value       : 120


gfsh>get --region=/Factorials --key=6 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 6
Value Class : java.lang.Long
Value       : 720


gfsh>get --region=/Factorials --key=4 --key-class=java.lang.Long
Result      : true
Key Class   : java.lang.Long
Key         : 4
Value Class : java.lang.Long
Value       : 24


gfsh>describe region --name=/Factorials
..........................................................
Name            : Factorials
Data Policy     : partition
Hosting Members : SpringBootGemFireServer

Non-Default Attributes Shared By Hosting Members

 Type  | Name | Value
------ | ---- | -----
Region | size | 6

```

As you can see, the "`Factorials`" *Region* size changes as we request different keys (numbers).

It is also a simple matter to adjust the *Spring* configuration and perhaps add eviction or expiration policies
(along with other settings) to prevent this *Region* from growing unbounded.

### Running with Apache Geode

It is a simple matter to run this example with *Apache Geode* instead of *Pivotal GemFire*.

You simply only need to adjust the [dependency](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/build.gradle#L42)
from `spring-data-gemfire` to `spring-data-geode`.

For your convenience, there is already a branch ("`apache-geode`") setup with *Apache Geode* configured.

Use an installation of *Apache Geode* to run *Locators* and *Servers* using _Gfsh_.

### Running with an Embedded GemFire/Geode Locator

While it is apparent you can form an external cluster and have our *Spring Boot GemFire Server* connect to this
existing cluster by defining the `gemfire.locators` System property ([here](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L70)),
it is also possible to start this example from a *Spring* perspective.  This is particularly useful in a microservices
-based application architecture hosted in a cloud environment (e.g. *Pivotal CloudFoundry*, which is even possible
with or without the *PCF GemFire* tile).

This is accomplished by running an "embedded" *GemFire Locator* (service) using the `start-gemfire` *GemFire* System property
([for example](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L75-L77))
in our *Spring Boot GemFire Server* application.

Additionally, it is also possible for our *Spring Boot GemFire Server* to become a *GemFire Manager*
(see [here](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L71-L73),
and specifically the [jmx-manager-start](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/java/org/example/SpringBootGemFireServer.java#L73)
*GemFire* System property, which is configured [here](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/resources/application.properties#L7)).
but also it is possible to run an

With this *GemFire* System property defined, when our *GemFire* member is started, it is possible to connect to
this member by using either the `--locator` or `--jmx-manager` command switch to the *Gfsh* `connect` command,
without any prior existing cluster or members.

Furthermore, once the *Spring Boot GemFire Server* is configured with `start-locator` and started, it is very easy to
add additional members in *Gfsh* by pointing members to the embedded *Locator* of our *Spring Boot GemFire Server*.

Before proceeding to the following example, please stop the previous cluster...

```
gfsh>stop locator --name=GemFireLocator
Stopping Locator running in /Users/jblum/pivdev/lab/GemFireLocator on 10.99.199.9[10334] as GemFireLocator...
Process ID: 81551
Log File: /Users/jblum/pivdev/lab/GemFireLocator/GemFireLocator.log
...

No longer connected to 10.99.199.9[1099].

No longer connected to 10.99.199.9[1099].


gfsh>list members
"list members" is not available. Reason: Requires connection.
```

Onwards...

#### Example

1. First, I set the `start-locator` *application.properties* [property](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/src/main/resources/application.properties#L8)
accordingly...

```
spring.gemfire.start-locator=localhost[10334]
```

2. Then, I start my *Spring Boot GemFire Server*...

```
$ gradlew bootRun
```

3. Next, I can connect to this server...

```
gfsh>connect --locator=localhost[10334]
Connecting to Locator at [host=localhost, port=10334] ..
Connecting to Manager at [host=10.99.199.9, port=1199] ..
Successfully connected to: [host=10.99.199.9, port=1199]


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632


gfsh>disconnect
Disconnecting from: 10.99.199.9[1199]
Disconnected from : 10.99.199.9[1199]


gfsh>connect --jmx-manager=localhost[1199]
Connecting to Manager at [host=localhost, port=1199] ..
Successfully connected to: [host=localhost, port=1199]


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632


gfsh>disconnect
Disconnecting from: localhost[1199]
Disconnected from : localhost[1199]


gfsh>connect
Connecting to Locator at [host=localhost, port=10334] ..
Connecting to Manager at [host=10.99.199.9, port=1199] ..
Successfully connected to: [host=10.99.199.9, port=1199]


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632

gfsh>
```

As you can see, I can connect 3 different ways:

1. `connect --locator=localhost[10334]`
2. `connect --jmx-manager=localhost[1199]`
3. `connect`

The last option (`connect`) just attempts to connect to a *Locator* on the default host and port
(i.e. `localhost[10334]`).

When Gfsh connects via a *Locator* (indirectly to the *Manager*) a request is made by the *Locator* to "locate" any
existing *Manager* in the cluster.  If one is found, then the *Locator* returns the coordinates of this *Manager*
to the JMX client (e.g. *Gfsh*).  If a existing *Manager* is not found, the *Locator* becomes a *Manager*.  Either way,
*Gfsh*, upon receiving the *Manager's* coordinates, will open a JMX (RMI) connection to the *Manager* directly
and therefore bypass the *Locator* on all subsequent requests (i.e. these are JMX/RMI protocol requests; not *Locator*
requests which uses a separate and distinct protocol).  This is why option 2 is the most direct route, when, of course,
the *Manager* coordinates are known in advance.

Now, you can proceed to add additional members (*Locators* and *Servers*, or other members serving as a *Manager*)...

```
gfsh>start locator --name=ExampleLocator --port=11235 --log-level=config
Starting a GemFire Locator in /Users/jblum/pivdev/lab/ExampleLocator...
.......
Locator in /Users/jblum/pivdev/lab/ExampleLocator on 10.99.199.9[11235] as ExampleLocator is currently online.
Process ID: 83108
Uptime: 3 seconds
GemFire Version: 8.2.0
Java Version: 1.8.0_72
Log File: /Users/jblum/pivdev/lab/ExampleLocator/ExampleLocator.log
JVM Arguments: -Dgemfire.default.locators=10.99.199.9:127.0.0.1[10334] -Dgemfire.enable-cluster-configuration=true -Dgemfire.load-cluster-configuration-from-dir=false -Dgemfire.log-level=config -Dgemfire.launcher.registerSignalHandlers=true -Djava.awt.headless=true -Dsun.rmi.dgc.server.gcInterval=9223372036854775806
Class-Path: /Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/gemfire.jar:/Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/locator-dependencies.jar

Cluster configuration service is up and running.


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632
ExampleLocator          | 10.99.199.9(ExampleLocator:83108:locator)<v1>:34644


gfsh>start server --name=ExampleServer --server-port=12480 --log-level=config
Starting a GemFire Server in /Users/jblum/pivdev/lab/ExampleServer...
....
Server in /Users/jblum/pivdev/lab/ExampleServer on 10.99.199.9[12480] as ExampleServer is currently online.
Process ID: 83117
Uptime: 2 seconds
GemFire Version: 8.2.0
Java Version: 1.8.0_72
Log File: /Users/jblum/pivdev/lab/ExampleServer/ExampleServer.log
JVM Arguments: -Dgemfire.default.locators=10.99.199.9[11235],10.99.199.9:127.0.0.1[10334] -Dgemfire.log-level=config -Dgemfire.use-cluster-configuration=true -XX:OnOutOfMemoryError=kill -KILL %p -Dgemfire.launcher.registerSignalHandlers=true -Djava.awt.headless=true -Dsun.rmi.dgc.server.gcInterval=9223372036854775806
Class-Path: /Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/gemfire.jar:/Users/jblum/Downloads/Pivotal/GemStone/Products/GemFire/Pivotal_GemFire_820_b17919_Linux/lib/server-dependencies.jar


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632
ExampleLocator          | 10.99.199.9(ExampleLocator:83108:locator)<v1>:34644
ExampleServer           | 10.99.199.9(ExampleServer:83117)<v2>:62276

```

Additionally, I can stop my "ExampleLocator" *Locator* member and the cluster is still there.

```
gfsh>stop locator --name=ExampleLocator
Stopping Locator running in /Users/jblum/pivdev/lab/ExampleLocator on 10.99.199.9[11235] as ExampleLocator...
Process ID: 83108
Log File: /Users/jblum/pivdev/lab/ExampleLocator/ExampleLocator.log
...


gfsh>list members
         Name           | Id
----------------------- | ---------------------------------------------------
SpringBootGemFireServer | 10.99.199.9(SpringBootGemFireServer:83057)<v0>:6632
ExampleServer           | 10.99.199.9(ExampleServer:83117)<v2>:62276

```

This is because the cluster is defined by the "embedded" *Locator* running in our *Spring Boot GemFire Server*.


### Notes

The only thing you cannot do with a *Spring*-configured *GemFire Server*, JVM process
is run `status server` or `stop server` in *Gfsh* on the member.

The main reason for this is because the *Spring (Boot) GemFire Server* node was not started
with *GemFire* infrastructure classes (namely, `o.a.g.distributed ServerLauncher`),
which registers a *Locator/Server* node with the *GemFire* management infrastructure
to be "managed" and controlled accordingly.

However, any *Spring*-configured node is visible from *Gfsh* and other tools
(e.g. *Pulse*, even *JConsole* or *JVisualVM*) and can be interacted with it accordingly.

### Conclusion

Clearly there are many different ways to start and form a *Pivotal GemFire* or *Apache Geode*
cluster with and without *Spring*. Hopefully this gives you a Springy way to get started quickly.

### Issues

For any issues or feedback on this project, please file a [GitHub Issue](https://github.com/jxblum/spring-boot-gemfire-server-example/issues).

The only known issue (at the moment) is when running the *Spring Boot GemFire Server* using the "executable" JAR file,
*Pivotal GemFire* and *Apache Geode* will log an `NoClassDefFoundError` stating that *GemFire/Geode* cannot find
the `org.apache.geode.logging.log4j.LogManager` class:

For instance...

```
java.lang.NoClassDefFoundError: org/apache/shiro/realm/Realm
	at org.apache.geode.management.internal.cli.commands.IndexCommands.<init>(IndexCommands.java:87)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at java.lang.Class.newInstance(Class.java:442)
	at org.apache.geode.management.internal.cli.CommandManager.loadGeodeCommands(CommandManager.java:228)
	at org.apache.geode.management.internal.cli.CommandManager.loadCommands(CommandManager.java:176)
	at org.apache.geode.management.internal.cli.CommandManager.<init>(CommandManager.java:83)
	at org.apache.geode.management.internal.cli.GfshParser.<init>(GfshParser.java:64)
	at org.apache.geode.management.internal.cli.remote.CommandProcessor.<init>(CommandProcessor.java:59)
	at org.apache.geode.management.internal.cli.remote.MemberCommandService.<init>(MemberCommandService.java:37)
	at org.apache.geode.management.cli.CommandService.createLocalCommandService(CommandService.java:127)
	at org.apache.geode.management.internal.beans.MemberMBeanBridge.<init>(MemberMBeanBridge.java:348)
	at org.apache.geode.management.internal.beans.ManagementAdapter.handleCacheCreation(ManagementAdapter.java:142)
	at org.apache.geode.management.internal.beans.ManagementListener.handleEvent(ManagementListener.java:106)
	at org.apache.geode.distributed.internal.InternalDistributedSystem.notifyResourceEventListeners(InternalDistributedSystem.java:2156)
	at org.apache.geode.distributed.internal.InternalDistributedSystem.handleResourceEvent(InternalDistributedSystem.java:546)
	at org.apache.geode.internal.cache.GemFireCacheImpl.initialize(GemFireCacheImpl.java:1183)
	at org.apache.geode.internal.cache.GemFireCacheImpl.basicCreate(GemFireCacheImpl.java:758)
	at org.apache.geode.internal.cache.GemFireCacheImpl.create(GemFireCacheImpl.java:745)
	at org.apache.geode.cache.CacheFactory.create(CacheFactory.java:173)
	at org.apache.geode.cache.CacheFactory.create(CacheFactory.java:212)
	at java.util.Optional.orElseGet(Optional.java:267)
	at org.springframework.data.gemfire.CacheFactoryBean.createCache(CacheFactoryBean.java:432)
	at org.springframework.data.gemfire.CacheFactoryBean.resolveCache(CacheFactoryBean.java:313)
	at org.springframework.data.gemfire.CacheFactoryBean.init(CacheFactoryBean.java:263)
	at java.util.Optional.orElseGet(Optional.java:267)
	at org.springframework.data.gemfire.CacheFactoryBean.getObject(CacheFactoryBean.java:739)
	at org.springframework.data.gemfire.CacheFactoryBean.getObject(CacheFactoryBean.java:106)
	at org.springframework.beans.factory.support.FactoryBeanRegistrySupport.doGetObjectFromFactoryBean(FactoryBeanRegistrySupport.java:161)
	at org.springframework.beans.factory.support.FactoryBeanRegistrySupport.getObjectFromFactoryBean(FactoryBeanRegistrySupport.java:101)
	at org.springframework.beans.factory.support.AbstractBeanFactory.getObjectForBeanInstance(AbstractBeanFactory.java:1645)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.getObjectForBeanInstance(AbstractAutowireCapableBeanFactory.java:1178)
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:322)
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200)
	at org.springframework.beans.factory.config.DependencyDescriptor.resolveCandidate(DependencyDescriptor.java:251)
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.doResolveDependency(DefaultListableBeanFactory.java:1138)
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.resolveDependency(DefaultListableBeanFactory.java:1065)
	at org.springframework.beans.factory.support.ConstructorResolver.resolveAutowiredArgument(ConstructorResolver.java:815)
	at org.springframework.beans.factory.support.ConstructorResolver.createArgumentArray(ConstructorResolver.java:721)
	at org.springframework.beans.factory.support.ConstructorResolver.instantiateUsingFactoryMethod(ConstructorResolver.java:470)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.instantiateUsingFactoryMethod(AbstractAutowireCapableBeanFactory.java:1250)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1099)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:545)
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:502)
	at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:312)
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:228)
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:310)
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200)
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:741)
	at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:868)
	at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:549)
	at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:140)
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:752)
	at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:388)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:327)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1246)
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1234)
	at org.example.SpringBootGemFireServer.main(SpringBootGemFireServer.java:39)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.springframework.boot.loader.MainMethodRunner.run(MainMethodRunner.java:48)
	at org.springframework.boot.loader.Launcher.launch(Launcher.java:87)
	at org.springframework.boot.loader.Launcher.launch(Launcher.java:50)
	at org.springframework.boot.loader.JarLauncher.main(JarLauncher.java:51)
Caused by: java.lang.ClassNotFoundException: org.apache.shiro.realm.Realm
	at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:357)

...

[info 2018/03/02 17:21:01.393 PST <main> tid=0x1] CacheServer Configuration:   port=40404
  max-connections=800 max-threads=0 notify-by-subscription=true socket-buffer-size=32768
  maximum-time-between-pings=60000 maximum-message-count=230000 message-time-to-live=180
  eviction-policy=none capacity=1 overflow directory=. groups=[] loadProbe=ConnectionCountProbe
  loadPollInterval=5000 tcpNoDelay=true
```

This is both unfortunate and misleading since as the last log statement indicates, the member starts up
and joins the cluster just fine.

Pivotal GemFire and Apache Geode uses a broken class loading mechanism when its own "custom", Java `java.lang.ClassLoader`
(i.e. [`org.apache.geode.internal.ClassPathLoader`](https://github.com/apache/geode/blob/rel/v1.4.0/geode-core/src/main/java/org/apache/geode/internal/ClassPathLoader.java))
is in use.  This is problematic when executing *GemFire* from an "executable" *Spring Boot* created JAR file
(i.e. a JAR file containing all the application dependencies).

_Spring Boot_ also uses a "custom" Java `java.lang.ClassLoader` of its own to load dependency JAR files contained in
the application JAR file itself.  However, *GemFire/Geode* knows nothing about the *Spring Boot* `ClassLoader`,
and even though the `aache-shiro-1.3.2.jar` is contained in the `BOOT-INF/lib/` sub-directory relative to
the root of "executable" JAR file (`spring-boot-gemfire-server-example-1.0.0.SNAPSHOT.jar`), it is not able to
find and resolve the *Apache Shiro* classes needed by *GemFire/Geode* classes running the server (a *Manager*),
and so this `NoClassDefFoundError` occurs when running with the "executable" application JAR file.

This problem does **not** occur when using the *Spring Boot Gradle Plugin* because the application lib dependencies
are not contained in a JAR file and instead, the application is just run from the command-line as if doing...

```
// classpath with log4j-api-2.8.2.jar

$ java ... -classpath=".." org.example.SpringBootGemFireServer ...
```

I.e. the *Spring Boot Gradle Plugin* just uses the [classpath](https://github.com/jxblum/spring-boot-gemfire-server-example/blob/master/build.gradle#L28-L37)
defined by *Gradle* as declared in `build.gradle` under the `dependencies` closure.


### Contributions

I welcome any contributions through [Pull Requests](https://github.com/jxblum/spring-boot-gemfire-server-example/pulls).
If you have an idea or improvement, please share.  No CLA necessary.

**Thanks**
