<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table;

use Keboola\Datatype\Definition\Synapse;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use PHPUnit\Framework\TestCase;

class SynapseTableDefinitionTest extends TestCase
{
    public function test(): void
    {
        $columns = new ColumnCollection([
            new SynapseColumn(
                'col1',
                new Synapse('NVARCHAR'),
            ),
        ]);
        $definition = new SynapseTableDefinition(
            'schema',
            'tableName',
            false,
            $columns,
            ['pk1'],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_COLUMNSTORE_INDEX),
        );
        self::assertSame('schema', $definition->getSchemaName());
        self::assertSame('tableName', $definition->getTableName());
        self::assertSame(['col1'], $definition->getColumnsNames());
        self::assertSame(['pk1'], $definition->getPrimaryKeysNames());
        self::assertSame('ROUND_ROBIN', $definition->getTableDistribution()->getDistributionName());
        self::assertSame('CLUSTERED COLUMNSTORE INDEX', $definition->getTableIndex()->getIndexType());
        self::assertSame($columns, $definition->getColumnsDefinitions());
        self::assertFalse($definition->isTemporary());
    }
}
