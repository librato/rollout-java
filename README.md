This project is a read-only client for [rollout](https://github.com/FetLife/rollout/), except that it uses
[Zookeeper](http://zookeeper.apache.org/) as the backend store instead of [Redis](http://redis.io/). At Librato, we
use this along side of the [rollout-zk](https://github.com/papertrail/rollout-zk) plugin.

## Getting started

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.librato.rollout</groupId>
    <artifactId>rollout</artifactId>
    <version>0.15</version>
</dependency>
```

If you don't have it already, also add [jackson](https://github.com/FasterXML/jackson)
as a dependency:

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.8.5</version>
</dependency>
```

First create your `CuratorFramework` and pass that into a new `RolloutZKClient` along with the Zookeeper path used in
rollout, then pass that along to a new `RolloutClient`:

```java
CuratorFramework framework = CuratorFrameworkFactory.builder()
        .connectString("localhost:2181")
        .retryPolicy(new RetryNTimes(1, 100))
        .build();
framework.start();

RolloutClient client = new RolloutZKClient(framework, "/rollout/users");
client.start(); // You must call start for this to connect to Zookeeper and set watches
```

Now you may use the client to confirm feature flags for any `long` ID and `List<String>` of groups for a given user:

```java
if (client.userFeatureActive("some_feature", myUser.getId(), myUser.getGroups()) {
    /* do something */
}
```

## Testing

First, you must have Zookeeper running locally on `localhost:2181`.

    mvn test
