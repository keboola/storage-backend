<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;

/**
 * @internal
 * @todo should be moved to utils
 */
final class TableDistribution
{
    public const TABLE_DISTRIBUTION_HASH = 'HASH';
    public const TABLE_DISTRIBUTION_REPLICATE = 'REPLICATE';
    public const TABLE_DISTRIBUTION_ROUND_ROBIN = 'ROUND_ROBIN';

    private string $distributionName;

    /** @var String[] */
    private array $distributionColumnsNames;

    /**
     * @param String[] $distributionColumnsNames
     */
    public function __construct(
        string $distributionName = self::TABLE_DISTRIBUTION_ROUND_ROBIN,
        array $distributionColumnsNames = [],
    ) {
        Assert::assertTableDistribution($distributionName);
        Assert::assertValidHashDistribution($distributionName, $distributionColumnsNames);

        $this->distributionName = $distributionName;
        $this->distributionColumnsNames = $distributionColumnsNames;
    }

    /**
     * @return String[]
     */
    public function getDistributionColumnsNames(): array
    {
        return $this->distributionColumnsNames;
    }

    public function getDistributionName(): string
    {
        return $this->distributionName;
    }

    public function isHashDistribution(): bool
    {
        return $this->distributionName === self::TABLE_DISTRIBUTION_HASH;
    }
}
