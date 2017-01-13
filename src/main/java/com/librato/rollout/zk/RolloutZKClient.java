package com.librato.rollout.zk;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.librato.rollout.RolloutClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of RolloutClient for use with Zookeeper, compatible with https://github.com/papertrail/rollout-zk
 */
public class RolloutZKClient implements RolloutClient {
    private static final Logger log = LoggerFactory.getLogger(RolloutZKClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Splitter splitter = Splitter.on('|');
    private final AtomicReference<Map<String, Entry>> features = new AtomicReference<Map<String, Entry>>();
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final CuratorFramework framework;
    private final NodeCache nodeCache;

    public RolloutZKClient(final CuratorFramework framework, final String rolloutPath) {
        Preconditions.checkNotNull(framework, "CuratorFramework cannot be null");
        Preconditions.checkArgument(rolloutPath != null && !rolloutPath.isEmpty(), "rolloutPath cannot be null or blank");
        this.framework = framework;
        this.nodeCache = new NodeCache(framework, rolloutPath);
    }

    @Override
    public int getPercentage(String feature) {
        final Entry entry = features.get().get(feature);
        if (entry == null) return 0;
        return entry.percentage;
    }

    @Override
    public boolean userFeatureActive(String feature, long userId) {
        return userFeatureActive(feature, userId, Collections.<String>emptyList());
    }

    @Override
    public boolean userFeatureActive(String feature, long userId, List<String> userGroups) {
        final Entry entry = features.get().get(feature);
        if (entry == null) return false;
        if (Math.abs(userId) % 100 < entry.percentage) {
            return true;
        }
        if (entry.groups.contains("all")) {
            return true;
        }
        if (entry.userIds.contains(userId)) {
            return true;
        }
        if (userGroups != null && !userGroups.isEmpty()) {
            for (String group : entry.groups) {
                if (userGroups.contains(group)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void start() throws Exception {
        if (!isStarted.compareAndSet(false, true)) {
            throw new RuntimeException("Service already started!");
        }
        if (framework.getState() != CuratorFrameworkState.STARTED) {
            throw new RuntimeException("CuratorFramework is not started!");
        }
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                build();
            }
        });
        nodeCache.start(true);
        build();
    }

    @Override
    public void stop() {
        if (!isStarted.compareAndSet(true, false)) {
            throw new RuntimeException("Service already stopped or never started!");
        }
        try {
            nodeCache.close();
        } catch (IOException ex) {
            log.warn("Error when closing NodeCache", ex);
        }
    }

    private void build() throws IOException {
        features.set(ImmutableMap.copyOf(parseData(nodeCache.getCurrentData().getData())));
    }

    private Map<String, Entry> parseData(byte[] data) throws IOException {
        Map<String, String> raw = mapper.readValue(data, new TypeReference<Map<String, String>>() {
        });
        ImmutableMap.Builder<String, Entry> bldr = ImmutableMap.builder();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            if (e.getKey().equals("feature:__features__")) continue; // ignore this non-feature special case
            String ftr = e.getKey().substring(8); // strip the pre-pended 'feature:'
            bldr.put(ftr, Entry.fromString(e.getValue(), ftr));
        }
        return bldr.build();
    }

    static class Entry {
        public final int percentage;
        public final List<Long> userIds;
        public final List<String> groups;

        Entry(int percentage, List<Long> userIds, List<String> groups) {
            this.percentage = percentage;
            this.userIds = userIds;
            this.groups = groups;
        }

        public static Entry fromString(String s, String key) {
            final String[] splitResult = Iterables.toArray(splitter.split(s), String.class);
            if (splitResult.length != 3) {
                throw new RuntimeException(String.format("Invalid format: %s, (length %d)", s, splitResult.length));
            }
            int percentage = 0;
            try {
                percentage = (int) Double.parseDouble(splitResult[0]);
            } catch (NumberFormatException ex) {
                log.warn("Couldn't parse `{}` as a double, ignoring percentage for key `{}`", splitResult[0], key);
            }
            final List<String> groups = Arrays.asList(splitResult[2].split(","));
            final List<Long> userIds = new ArrayList<Long>();
            for (String id : splitResult[1].split(",")) {
                if (id.isEmpty()) continue;
                try {
                    userIds.add(Long.valueOf(id));
                } catch (NumberFormatException ex) {
                    log.warn("Couldn't parse `{}` as a long, ignoring user id for key `{}`", id, key);
                }
            }
            return new Entry(percentage, userIds, groups);
        }
    }
}
