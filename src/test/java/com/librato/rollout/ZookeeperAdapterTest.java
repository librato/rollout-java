package com.librato.rollout;

import com.google.common.collect.Lists;
import com.librato.rollout.zk.ZookeeperAdapter;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZookeeperAdapterTest {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperAdapterTest.class);
    private static final String rolloutPath = "/rollout-test-node";
    private static CuratorFramework framework;

    @BeforeClass
    public static void classSetup() throws Exception {
        framework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(new RetryNTimes(1, 100))
                .build();
        framework.start();

        framework.create().withMode(CreateMode.EPHEMERAL).forPath(rolloutPath, "{}".getBytes());
    }

    @AfterClass
    public static void classTeardown() throws Exception {
        framework.close();
    }

    @Test
    public void testUserIDFeatureActive() throws Exception {
        User user1 = new User(1, Lists.newArrayList("foo"));
        User user2 = new User(2, Lists.newArrayList( "bar"));
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1|\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertFalse(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1,2|\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertTrue(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testUserGroupsFeatureActive() throws Exception {
        User user1 = new User(1, Lists.newArrayList("foo"));
        User user2 = new User(2, Lists.newArrayList( "bar"));
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0||foo\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertFalse(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0||foo,bar\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertTrue(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testUserPercentageFeatureActive() throws Exception {
        User user1 = new User(1, Lists.newArrayList("foo"));
        User user2 = new User(2, Lists.newArrayList( "bar"));
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"25||\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertFalse(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"50||\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", user1));
            assertTrue(client.userFeatureActive("hello", user2));
            assertFalse(client.userFeatureActive("nosuchfeature", user1));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testInvalidJSON() throws Exception {
        User user1 = new User(1, Lists.newArrayList("foo"));
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            framework.setData().forPath(rolloutPath, "not json".getBytes());
            Thread.sleep(100);
            assertFalse(client.userFeatureActive("nosuchfeature", user1));

            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1|\"}".getBytes());
            Thread.sleep(100);

            assertTrue(client.userFeatureActive("hello", user1));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    class User implements RolloutUser {
        private final long id;
        private final List<String> groups;

        public User(long id, List<String> groups) {
            this.id = id;
            this.groups = groups;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public List<String> getGroups() {
            return groups;
        }
    }
}