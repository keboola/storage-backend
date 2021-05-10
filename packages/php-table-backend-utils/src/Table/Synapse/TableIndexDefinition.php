<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Synapse;

/**
 * @todo this class is missing validation for CLUSTERED INDEX now, but this is not yet used
 */
final class TableIndexDefinition
{
    public const TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX = 'CLUSTERED COLUMNSTORE INDEX';
    public const TABLE_INDEX_TYPE_HEAP = 'HEAP';
    public const TABLE_INDEX_TYPE_CLUSTERED_INDEX = 'CLUSTERED INDEX';

    public const AVAILABLE_TABLE_INDEXES = [
        self::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX,
        self::TABLE_INDEX_TYPE_HEAP,
        self::TABLE_INDEX_TYPE_CLUSTERED_INDEX,
    ];

    /** @var self::TABLE_INDEX_TYPE_* */
    private $indexType;

    /** @var String[] */
    private $indexedColumnsNames;

    /**
     * @param self::TABLE_INDEX_TYPE_* $indexType
     * @param String[] $indexedColumnsNames
     */
    public function __construct(
        string $indexType,
        array $indexedColumnsNames = []
    ) {
        Assert::assertTableIndex($indexType);
        Assert::assertValidClusteredIndex($indexType, $indexedColumnsNames);

        $this->indexType = $indexType;
        $this->indexedColumnsNames = $indexedColumnsNames;
    }

    /**
     * @return self::TABLE_INDEX_TYPE_*
     */
    public function getIndexType(): string
    {
        return $this->indexType;
    }

    /**
     * @return String[]
     */
    public function getIndexedColumnsNames(): array
    {
        return $this->indexedColumnsNames;
    }
}
