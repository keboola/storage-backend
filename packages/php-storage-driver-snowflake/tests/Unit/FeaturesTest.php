<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Unit;

use Keboola\StorageDriver\Snowflake\Features;
use PHPUnit\Framework\TestCase;

class FeaturesTest extends TestCase
{
    public function testIsFeatureInListPositive(): void
    {
        $features = [
            Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            Features::FEATURE_PAY_AS_YOU_GO,
        ];

        $this->assertTrue(Features::isFeatureInList($features, Features::FEATURE_PAY_AS_YOU_GO));
        $this->assertTrue(Features::isFeatureInList($features, Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE));
    }

    public function testIsFeatureInListNegative(): void
    {
        $features = [
            Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            Features::FEATURE_PAY_AS_YOU_GO,
        ];

        $this->assertFalse(Features::isFeatureInList($features, Features::FEATURE_PROTECTED_DEFAULT_BRANCH));
        $this->assertFalse(Features::isFeatureInList([], Features::FEATURE_PAY_AS_YOU_GO));
    }

    public function testIsOneOfFeaturesInListPositive(): void
    {
        $features = [
            Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            Features::FEATURE_PAY_AS_YOU_GO,
        ];

        $this->assertTrue(Features::isOneOfFeaturesInList($features, [
            Features::FEATURE_PROTECTED_DEFAULT_BRANCH,
            Features::FEATURE_PAY_AS_YOU_GO,
        ])); // intersects on pay-as-you-go

        $this->assertTrue(Features::isOneOfFeaturesInList($features, [
            Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
        ]));
    }

    public function testIsOneOfFeaturesInListNegative(): void
    {
        $features = [
            Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            Features::FEATURE_PAY_AS_YOU_GO,
        ];

        $this->assertFalse(Features::isOneOfFeaturesInList($features, [
            Features::FEATURE_PROTECTED_DEFAULT_BRANCH,
            Features::FEATURE_REAL_STORAGE_BRANCHES,
        ]));
    }

    public function testIsOneOfFeaturesInListEmptyList(): void
    {
        $this->assertFalse(Features::isOneOfFeaturesInList([], [
            Features::FEATURE_PROTECTED_DEFAULT_BRANCH,
        ]));

        $this->assertFalse(Features::isOneOfFeaturesInList([], [])); // no features anywhere
    }
}
