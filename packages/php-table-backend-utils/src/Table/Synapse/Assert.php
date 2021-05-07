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

    /**
     * @param string $tableDistributionName
     * @param string[] $hashDistributionColumnsNames
     */
    public static function assertValidHashDistribution(
        string $tableDistributionName,
        array $hashDistributionColumnsNames
    ): void {
        if ($tableDistributionName === TableDistributionDefinition::TABLE_DISTRIBUTION_HASH
            && count($hashDistributionColumnsNames) !== 1
        ) {
            throw new LogicException('HASH table distribution must have one distribution key specified.');
        }
    }

    public static function assertTableDistribution(string $tableDistributionName): void
    {
        if (!in_array($tableDistributionName, [
            TableDistributionDefinition::TABLE_DISTRIBUTION_HASH,
            TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN,
            TableDistributionDefinition::TABLE_DISTRIBUTION_REPLICATE,
        ], true)) {
            throw new LogicException(sprintf(
                'Unknown table distribution: "%s" specified.',
                $tableDistributionName
            ));
        }
    }
}
