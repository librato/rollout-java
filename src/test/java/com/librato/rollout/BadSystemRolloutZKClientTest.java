package com.librato.rollout;

import com.librato.rollout.zk.RolloutZKClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class BadSystemRolloutZKClientTest {

    private static final String rolloutPath = "/rollout-test-node";
    private static CuratorFramework framework;

    @BeforeClass
    public static void classSetup() {
        framework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .retryPolicy(new RetryNTimes(1, 100))
                .build();
        framework.start();
    }

    // This does what the main test class does, but doesn't do the @BeforeClass setup
    // first -- easier by far than doing 'interesting' things over there

    @Test(expected = IOException.class)
    public void explodesOnNoZKPath() throws Exception {
        final RolloutClient client = new RolloutZKClient(framework, rolloutPath);
        client.start();
    }

}
