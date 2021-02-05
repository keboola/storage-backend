<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

/**
 * @internal
 */
final class DestinationTableOptions
{
    /** @var String[] */
    private $columnNamesInOrder;

    /** @var String[] */
    private $primaryKeys;

    /** @var TableDistribution */
    private $distribution;

    /**
     * @param String[] $columnNamesInOrder
     * @param String[] $primaryKeys
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

    public function getColumnNamesInOrder(): array
    {
        return $this->columnNamesInOrder;
    }

    public function getDistribution(): TableDistribution
    {
        return $this->distribution;
    }

    public function getPrimaryKeys(): array
    {
        return $this->primaryKeys;
    }
}
