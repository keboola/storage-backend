<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

/**
 * @internal
 */
final class DestinationTableOptions
{
    public const TABLE_DISTRIBUTION_HASH = 'HASH';
    public const TABLE_DISTRIBUTION_ROUND_ROBIN = 'ROUND_ROBIN';

    /** @var String[] */
    private $columnNamesInOrder;

    /** @var String[] */
    private $primaryKeys;

    /** @var string */
    private $distribution;

    /** @var String[] */
    private $distributionColumnsNames;

    /**
     * @param String[] $columnNamesInOrder
     * @param String[] $primaryKeys
     * @param String[] $distributionColumnsNames
     */
    public function __construct(
        array $columnNamesInOrder,
        array $primaryKeys,
        string $distribution = self::TABLE_DISTRIBUTION_ROUND_ROBIN,
        array $distributionColumnsNames = []
    ) {
        $this->columnNamesInOrder = $columnNamesInOrder;
        $this->primaryKeys = $primaryKeys;
        $this->distribution = $distribution;
        $this->distributionColumnsNames = $distributionColumnsNames;
    }

    public function getColumnNamesInOrder(): array
    {
        return $this->columnNamesInOrder;
    }

    public function getDistribution(): string
    {
        return $this->distribution;
    }

    /**
     * @return String[]
     */
    public function getDistributionColumnsNames(): array
    {
        return $this->distributionColumnsNames;
    }

    public function getPrimaryKeys(): array
    {
        return $this->primaryKeys;
    }
}
