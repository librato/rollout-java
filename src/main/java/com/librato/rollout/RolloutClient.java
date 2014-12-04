package com.librato.rollout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * TODO: Document
 */
public class RolloutClient {
    private static final Logger log = LoggerFactory.getLogger(RolloutClient.class);
    private final RolloutAdapter adapter;

    public RolloutClient(RolloutAdapter adapter) {
        this.adapter = adapter;
    }

    public boolean userFeatureActive(final String feature, final long userId, List<String> userGroups) {
        return adapter.userFeatureActive(feature, userId, userGroups);
    }

    public int getPercentage(final String feature) {
        return adapter.getPercentage(feature);
    }
}
