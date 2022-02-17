<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\SynapseNext;

use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class SynapseBaseTestCase extends \Tests\Keboola\Db\ImportExportFunctional\Synapse\SynapseBaseTestCase
{
    /**
     * @param string[] $columnsNames
     */
    public function getColumnsWithoutTypes(array $columnsNames): ColumnCollection
    {
        $columns = array_map(function ($colName) {
            return new SynapseColumn(
                $colName,
                new Synapse(
                    'NVARCHAR',
                    ['length' => 4000]
                )
            );
        }, $columnsNames);
        return new ColumnCollection($columns);
    }

    /**
     * @param string[] $columns
     * @param string[] $pks
     */
    public function getGenericTableDefinition(
        string $schemaName,
        string $tableName,
        array $columns,
        array $pks = []
    ): SynapseTableDefinition {
        return new SynapseTableDefinition(
            $schemaName,
            $tableName,
            false,
            $this->getColumnsWithoutTypes($columns),
            $pks,
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN, []),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX, [])
        );
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertSynapseTableEqualsExpected(
        SourceInterface $source,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        ))->getColumnsNames();

        if ($options->useTimestamp()) {
            self::assertContains('_timestamp', $tableColumns);
        } else {
            self::assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $source->getColumnsNames())) {
            $tableColumns = array_filter($tableColumns, function ($column) {
                return $column !== '_timestamp';
            });
        }

        $tableColumns = array_map(function ($column) {
            return sprintf('[%s]', $column);
        }, $tableColumns);

        $sql = sprintf(
            'SELECT %s FROM [%s].[%s]',
            implode(', ', $tableColumns),
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        $queryResult = array_map(function ($row) {
            return array_map(function ($column) {
                return $column;
            }, array_values($row));
        }, $this->connection->fetchAllAssociative($sql));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message
        );
    }
}
