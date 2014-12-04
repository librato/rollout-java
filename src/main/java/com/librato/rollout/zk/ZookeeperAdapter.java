package com.librato.rollout.zk;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.librato.rollout.RolloutAdapter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
                if (framework.getState() != CuratorFrameworkState.STARTED || !isStarted.get() || event.getType() != CuratorEventType.WATCHED || !rolloutPath.equals(event.getPath())) {
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
    public int getPercentage(String feature) {
        final String[] splitResult = split(feature);
        if (splitResult == null) {
            return 0;
        }
        return Integer.parseInt(splitResult[0]);
    }

    private String[] split(String feature) {
        final String value = features.get().get(String.format("feature:%s", feature));
        if (value == null) {
            return null;
        }
        final String[] splitResult = Iterables.toArray(splitter.split(value), String.class);
        if (splitResult.length != 3) {
            throw new RuntimeException(String.format("Invalid format: %s, (length %d)", value, splitResult.length));
        }
        return splitResult;
    }

    @Override
    public boolean userFeatureActive(String feature, long userId, List<String> userGroups) {
        final String[] splitResult = split(feature);
        if (splitResult == null) {
            return false;
        }
        // Check percentage first as it's most efficient
        final int percentage = Integer.parseInt(splitResult[0]);
        if (userId % 100 < percentage) {
            return true;
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
        if (framework.getState() != CuratorFrameworkState.STARTED) {
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
