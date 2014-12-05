package com.librato.rollout;

import java.util.List;

public interface RolloutClient {
    /**
     * @param feature    Rollout feature
     * @param userId     id of the user for use when checking ids and percentages
     * @param userGroups groups to check against
     * @return true if feature is active based on user id, percentage, or group
     */
    public boolean userFeatureActive(final String feature, long userId, List<String> userGroups);

    /**
     * @param feature Rollout feature
     * @return Percentage of given feature, or 0 if feature does not exist
     */
    public int getPercentage(final String feature);

    /**
     * Start the client; must be called before checking features
     *
     * @throws Exception
     */
    public void start() throws Exception;

    /**
     * Stop the client
     *
     * @throws Exception
     */
    public void stop() throws Exception;
}
