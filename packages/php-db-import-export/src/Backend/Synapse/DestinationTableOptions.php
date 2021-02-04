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

    /**
     * @param String[] $columnNamesInOrder
     * @param String[] $primaryKeys
     */
    public function __construct(
        array $columnNamesInOrder,
        array $primaryKeys
    ) {
        $this->columnNamesInOrder = $columnNamesInOrder;
        $this->primaryKeys = $primaryKeys;
    }

    public function getColumnNamesInOrder(): array
    {
        return $this->columnNamesInOrder;
    }

    public function getPrimaryKeys(): array
    {
        return $this->primaryKeys;
    }
}
