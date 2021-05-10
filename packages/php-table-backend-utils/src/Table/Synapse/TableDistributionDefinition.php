<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Synapse;

final class TableDistributionDefinition
{
    public const TABLE_DISTRIBUTION_HASH = 'HASH';
    public const TABLE_DISTRIBUTION_REPLICATE = 'REPLICATE';
    public const TABLE_DISTRIBUTION_ROUND_ROBIN = 'ROUND_ROBIN';

    public const AVAILABLE_TABLE_DISTRIBUTIONS = [
        self::TABLE_DISTRIBUTION_HASH,
        self::TABLE_DISTRIBUTION_REPLICATE,
        self::TABLE_DISTRIBUTION_ROUND_ROBIN,
    ];

    /** @var string */
    private $distributionName;

    /** @var String[] */
    private $distributionColumnsNames;

    /**
     * @param self::TABLE_DISTRIBUTION_* $distributionName
     * @param String[] $distributionColumnsNames
     */
    public function __construct(
        string $distributionName,
        array $distributionColumnsNames = []
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
