<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

final class DestinationTableOptions
{
    public const PRIMARY_KEYS_DEFINITION_METADATA = 'METADATA';
    public const PRIMARY_KEYS_DEFINITION_DB = 'DB';

    /** @var String[] */
    private $columnNamesInOrder;

    /** @var String[] */
    private $primaryKeys;

    /** @var string */
    private $primaryKeysDefinition;

    /**
     * @param String[] $columnNamesInOrder
     * @param String[] $primaryKeys
     */
    public function __construct(
        array $columnNamesInOrder,
        array $primaryKeys,
        string $primaryKeysDefinition
    ) {
        $this->columnNamesInOrder = $columnNamesInOrder;
        $this->primaryKeys = $primaryKeys;
        $this->primaryKeysDefinition = $primaryKeysDefinition;
    }

    public function isPrimaryKeyFromMetadata(): bool
    {
        return $this->primaryKeysDefinition === self::PRIMARY_KEYS_DEFINITION_METADATA;
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
