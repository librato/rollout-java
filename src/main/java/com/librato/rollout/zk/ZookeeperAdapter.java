package com.librato.rollout.zk;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.librato.rollout.RolloutAdapter;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: Document
 */
public class ZookeeperAdapter implements RolloutAdapter {
    private static final Logger log = LoggerFactory.getLogger(ZookeeperAdapter.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Splitter splitter = Splitter.on('|');
    private final AtomicReference<Map<String, String>> features = new AtomicReference<Map<String, String>>();
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final CuratorFramework framework;
    private final String rolloutPath;
    private final CuratorListener listener;

    public ZookeeperAdapter(final CuratorFramework framework, final String rolloutPath) {
        Preconditions.checkNotNull(framework, "CuratorFramework cannot be null");
        Preconditions.checkArgument(rolloutPath != null && !rolloutPath.isEmpty(), "rolloutPath cannot be null or blank");
        this.framework = framework;
        this.rolloutPath = rolloutPath;
        this.listener = new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                if (!framework.isStarted() || !isStarted.get() || event.getType() != CuratorEventType.WATCHED || !rolloutPath.equals(event.getPath())) {
                    return;
                }
                try {
                    getAndSet();
                } catch (Exception ex) {
                    log.error("Error on event update", ex);
                } finally {
                    // Set the watch just in case it was simply bad data
                    setWatch();
                }
            }
        };
    }

    @Override
    public boolean userFeatureActive(String feature, long userId, List<String> userGroups) {
        // TODO: This whole method is inefficient as it parses the data every time
        final String value = features.get().get(String.format("feature:%s", feature));
        if (value == null) {
            return false;
        }
        final String[] splitResult = Iterables.toArray(splitter.split(value), String.class);

        if (splitResult.length != 3) {
            log.warn("Invalid format: {}, (length {})", value, splitResult.length);
            return false;
        }

        final List<String> groups = Arrays.asList(splitResult[2].split(","));
        // Short-circuit
        if (groups.contains("all")) {
            return true;
        }

        // Check user ID
        final List<String> userIds = Arrays.asList(splitResult[1].split(","));
        final String uid = String.valueOf(userId);
        if (userIds.contains(uid)) {
            return true;
        }

        // Next, check percentage
        final int percentage = Integer.parseInt(splitResult[0]);
        if (userId % 10 < percentage / 10) {
            return true;
        }

        // Lastly, check groups
        if (userGroups != null && !userGroups.isEmpty()) {
            for (String group : groups) {
                if (userGroups.contains(group)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void start() throws Exception {
        if (!isStarted.compareAndSet(false, true)) {
            throw new RuntimeException("Service already started!");
        }
        if (!framework.isStarted()) {
            throw new RuntimeException("CuratorFramework is not started!");
        }
        // We initialize the value here to mitigate the race condition of watching the path and attempting to get a value
        // from the map
        getAndSet();
        framework.getCuratorListenable().addListener(listener);
        setWatch();
    }

    public void stop() {
        if (!isStarted.compareAndSet(true, false)) {
            throw new RuntimeException("Service already stopped or never started!");
        }
        framework.getCuratorListenable().removeListener(listener);
    }

    private void setWatch() throws Exception {
        framework.getData().watched().inBackground().forPath(rolloutPath);
    }

    private void getAndSet() throws Exception {
        features.set(ImmutableMap.copyOf(parseData(framework.getData().forPath(rolloutPath))));
    }

    private Map<String, String> parseData(byte[] data) throws IOException {
        return mapper.readValue(data, new TypeReference<Map<String, String>>() {});
    }
}
