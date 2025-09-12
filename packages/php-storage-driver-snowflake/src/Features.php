<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

/**
 * Class containing feature flags used in Snowflake driver
 */
class Features
{
    public const FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE = 'input-mapping-read-only-storage';
    public const FEATURE_PROTECTED_DEFAULT_BRANCH = 'protected-default-branch';
    public const FEATURE_REAL_STORAGE_BRANCHES = 'storage-branches';
    public const FEATURE_PAY_AS_YOU_GO = 'pay-as-you-go';

    /**
     * @param string[] $listOfFeatures
     * @param Features::FEATURE_* $feature
     */
    public static function isFeatureInList(array $listOfFeatures, string $feature): bool
    {
        return in_array($feature, $listOfFeatures, true);
    }

    /**
     * @param string[] $listOfFeatures
     * @param array<int,Features::FEATURE_*> $features
     */
    public static function isOneOfFeaturesInList(array $listOfFeatures, array $features): bool
    {
        return count(array_intersect($features, $listOfFeatures)) > 0;
    }
}
