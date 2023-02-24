<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

/**
 * @internal
 */
final class DestinationTableOptions
{
    /** @var string[] */
    private array $columnNamesInOrder;

    /** @var string[] */
    private array $primaryKeys;

    private TableDistribution $distribution;

    /**
     * @param string[] $columnNamesInOrder
     * @param string[] $primaryKeys
     */
    public function __construct(
        array $columnNamesInOrder,
        array $primaryKeys,
        TableDistribution $distribution
    ) {
        $this->columnNamesInOrder = $columnNamesInOrder;
        $this->primaryKeys = $primaryKeys;
        $this->distribution = $distribution;
    }

    /**
     * @return string[]
     */
    public function getColumnNamesInOrder(): array
    {
        return $this->columnNamesInOrder;
    }

    public function getDistribution(): TableDistribution
    {
        return $this->distribution;
    }

    /**
     * @return string[]
     */
    public function getPrimaryKeys(): array
    {
        return $this->primaryKeys;
    }
}
