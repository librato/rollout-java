package com.librato.rollout;

import java.util.List;

/**
 * TODO: Document
 */
public interface RolloutAdapter {
    public boolean userFeatureActive(final String feature, long userId, List<String> userGroups);
}
