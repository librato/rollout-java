package com.librato.rollout;

/**
 * TODO: Document
 */
public interface RolloutAdapter {
    public boolean userFeatureActive(final String feature, RolloutUser user);
}
