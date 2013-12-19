This project is a read-only client for [rollout](https://github.com/FetLife/rollout/), except that it uses
[Zookeeper](http://zookeeper.apache.org/) as the backend store instead of [Redis](http://redis.io/). At Librato, we
plan to use this along side of the [rollout-zk](https://github.com/papertrail/rollout-zk) plugin.

**Note**: This is using an old version of Curator as the backend for Zookeeper because we use Storm 0.8 in production
and have inherited it. It should be simple to update this to a newer Curator should anyone require it. Patches are
welcome.

## Getting started

Add the dependency to your `pom.xml`:

    <dependency>
        <groupId>com.librato.rollout</groupId>
        <artifactId>rollout</artifactId>
        <version>0.3</version>
    </dependency>

First create your `CuratorFramework` and pass that into a new `ZookeeperAdapter` along with the Zookeeper path used in
rollout, then pass that along to a new `RolloutClient`:

    CuratorFramework framework = CuratorFrameworkFactory.builder()
            .connectString("localhost:2181")
            .retryPolicy(new RetryNTimes(1, 100))
            .build();
    framework.start();

    ZookeeperAdapter adapter = new ZookeeperAdapter(framework, "/rollout/users");
    adapter.start(); // You must call start for this to connect to Zookeeper and set watches
    RolloutClient client = new RolloutClient(adapter);

Now you may use the client to confirm feature flags for any `long` ID and `List<String>` of groups for a given user:

    if (client.userFeatureActive("some_feature", myUser.getId(), myUser.getGroups()) {
        /* do something */
    }

## Testing

First, you must have Zookeeper running locally on `localhost:2181`.

    mvn test
