<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Synapse;

use LogicException;

final class Assert
{
    public static function assertTableIndex(string $indexName): void
    {
        if (!in_array($indexName, TableIndexDefinition::AVAILABLE_TABLE_INDEXES, true)) {
            throw new LogicException(sprintf(
                'Unknown table index type: "%s" specified. Available types are %s.',
                $indexName,
                implode('|', TableIndexDefinition::AVAILABLE_TABLE_INDEXES)
            ));
        }
    }

    /**
     * @param String[] $indexedColumnsNames
     */
    public static function assertValidClusteredIndex(string $indexName, array $indexedColumnsNames): void
    {
        if ($indexName === TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX
            && count($indexedColumnsNames) !== 1
        ) {
            throw new LogicException('CLUSTERED table index must have one key specified.');
        }
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
        if (!in_array($tableDistributionName, TableDistributionDefinition::AVAILABLE_TABLE_DISTRIBUTIONS, true)) {
            throw new LogicException(sprintf(
                'Unknown table distribution: "%s" specified. Available distributions are %s.',
                $tableDistributionName,
                implode('|', TableDistributionDefinition::AVAILABLE_TABLE_DISTRIBUTIONS)
            ));
        }
    }
}
