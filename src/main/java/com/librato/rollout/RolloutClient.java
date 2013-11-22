package com.librato.rollout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Document
 */
public class RolloutClient {
    private static final Logger log = LoggerFactory.getLogger(RolloutClient.class);
    private final RolloutAdapter adapter;

    public RolloutClient(RolloutAdapter adapter) {
        this.adapter = adapter;
    }

    public boolean userFeatureActive(final String feature, final RolloutUser user) {
        return adapter.userFeatureActive(feature, user);
    }
}
