<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Synapse;

use LogicException;

final class Assert
{
    public static function assertTableIndex(string $indexName): void
    {
        if (!in_array($indexName, [
            TableIndexDefinition::TABLE_INDEX_TYPE_CCI,
            TableIndexDefinition::TABLE_INDEX_TYPE_HEAP,
            TableIndexDefinition::TABLE_INDEX_TYPE_CI,
        ], true)) {
            throw new LogicException(sprintf(
                'Unknown table index type: "%s" specified.',
                $indexName
            ));
        }
    }

    /**
     * @param String[] $indexedColumnsNames
     */
    public static function assertValidClusteredIndex(string $indexName, array $indexedColumnsNames): void
    {
    }
}
