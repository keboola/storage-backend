<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

/**
 * @internal
 * @todo should be moved to utils
 */
final class TableDistribution
{
    public const TABLE_DISTRIBUTION_HASH = 'HASH';
    public const TABLE_DISTRIBUTION_ROUND_ROBIN = 'ROUND_ROBIN';

    /** @var string */
    private $distributionName;

    /** @var String[] */
    private $distributionColumnsNames;

    /**
     * @param String[] $distributionColumnsNames
     */
    public function __construct(
        string $distributionName = self::TABLE_DISTRIBUTION_ROUND_ROBIN,
        array $distributionColumnsNames = []
    ) {
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
