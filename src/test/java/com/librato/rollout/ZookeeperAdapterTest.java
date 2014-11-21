package com.librato.rollout;

import com.google.common.collect.Lists;
import com.librato.rollout.zk.ZookeeperAdapter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZookeeperAdapterTest {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperAdapterTest.class);
    private static final String rolloutPath = "/rollout-test-node";
    private static final Random rand = new Random();
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
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1|\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertFalse(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1,2|\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertTrue(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testUserGroupsFeatureActive() throws Exception {
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0||foo\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertFalse(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0||foo,bar\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertTrue(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testUserAllActive() throws Exception {
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0||all\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertTrue(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testUserPercentageFeatureActive() throws Exception {
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"25||\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 100, Lists.newArrayList("foo")));
            assertTrue(client.userFeatureActive("hello", 124, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("hello", 125, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"50||\"}".getBytes());
            Thread.sleep(100);
            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
            assertTrue(client.userFeatureActive("hello", 2, Lists.newArrayList("bar")));
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));

            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"100||\"}".getBytes());
            Thread.sleep(100);
            for (int i = 0; i < 10000; i++) {
                assertTrue(client.userFeatureActive("hello", rand.nextInt(), Collections.<String>emptyList()));
            }
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testGetPercentage() throws Exception {
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            assertEquals(0, client.getPercentage("nosuchfeature"));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"25||\"}".getBytes());
            Thread.sleep(100);
            assertEquals(0, client.getPercentage("nosuchfeature"));
            assertEquals(25, client.getPercentage("hello"));
            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"50||\"}".getBytes());
            Thread.sleep(100);
            assertEquals(0, client.getPercentage("nosuchfeature"));
            assertEquals(50, client.getPercentage("hello"));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }

    @Test
    public void testInvalidJSON() throws Exception {
        RolloutClient client;
        ZookeeperAdapter adapter = null;
        try {
            adapter = new ZookeeperAdapter(framework, rolloutPath);
            adapter.start();
            client = new RolloutClient(adapter);

            framework.setData().forPath(rolloutPath, "not json".getBytes());
            Thread.sleep(100);
            assertFalse(client.userFeatureActive("nosuchfeature", 1, Lists.newArrayList("foo")));

            framework.setData().forPath(rolloutPath, "{\"feature:hello\": \"0|1|\"}".getBytes());
            Thread.sleep(100);

            assertTrue(client.userFeatureActive("hello", 1, Lists.newArrayList("foo")));
        } finally {
            if (adapter != null) {
                adapter.stop();
            }
        }
    }
}
