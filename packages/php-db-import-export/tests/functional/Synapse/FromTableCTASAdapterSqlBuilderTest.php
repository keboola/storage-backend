<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\FromTableCTASAdapterSqlBuilder;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class FromTableCTASAdapterSqlBuilderTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getSourceSchema());
        $this->dropAllWithinSchema($this->getDestinationSchema());
        $this->createSchema();
    }

    protected function createSchema(): void
    {
        $this->connection->executeStatement(sprintf('CREATE SCHEMA %s', $this->getSourceSchema()));
        $this->connection->executeStatement(sprintf('CREATE SCHEMA %s', $this->getDestinationSchema()));
    }

    /**
     * @param SynapseColumn[] $cols
     */
    private function getTableDefinition(string $schemaName, string $tableName, array $cols): SynapseTableDefinition
    {
        return new SynapseTableDefinition(
            $schemaName,
            $tableName,
            false,
            new ColumnCollection($cols),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP)
        );
    }

    private function insertData(string $schemaName, string $tableName): void
    {
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                $schemaName,
                $tableName
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (1,1,\'1\',\'1\')',
                $schemaName,
                $tableName
            )
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO [%s].[%s]([pk1],[pk2],[col1],[col2]) VALUES (2,2,\'2\',\'2\')',
                $schemaName,
                $tableName
            )
        );
    }

    /**
     * @return \Generator<string,mixed>
     */
    public function ctasProvider(): \Generator
    {
        yield 'simple no casting (typed tables but varchar)' => [
            'sourceColumns' => $this->getDefaultColumns(),
            'expectedDestination' => $this->getTableDefinition(
                $this->getDestinationSchema(),
                'dest',
                $this->getDefaultColumns()
            ),
            'requireSameTables' => SynapseImportOptions::SAME_TABLES_REQUIRED,
            // phpcs:ignore
            'expectedSql' => 'CREATE TABLE [destination].[dest] WITH (DISTRIBUTION = ROUND_ROBIN,HEAP) AS SELECT [pk1], [pk2], [col1], [col2] FROM [source].[source]',
        ];

        yield 'simple no casting custom index and distribution (typed tables but varchar)' => [
            'sourceColumns' => $this->getDefaultColumns(),
            'expectedDestination' => new SynapseTableDefinition(
                $this->getDestinationSchema(),
                'dest',
                false,
                new ColumnCollection($this->getDefaultColumns()),
                [],
                new TableDistributionDefinition(
                    TableDistributionDefinition::TABLE_DISTRIBUTION_HASH,
                    ['pk1']
                ),
                new TableIndexDefinition(
                    TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX,
                    ['pk1']
                )
            ),
            'requireSameTables' => SynapseImportOptions::SAME_TABLES_REQUIRED,
            // phpcs:ignore
            'expectedSql' => 'CREATE TABLE [destination].[dest] WITH (DISTRIBUTION = HASH([pk1]),CLUSTERED INDEX([pk1])) AS SELECT [pk1], [pk2], [col1], [col2] FROM [source].[source]',
        ];

        yield 'simple with casting (not typed tables but varchar)' => [
            'sourceColumns' => $this->getDefaultColumns(),
            'expectedDestination' => $this->getTableDefinition(
                $this->getDestinationSchema(),
                'dest',
                $this->getDefaultColumns()
            ),
            'requireSameTables' => SynapseImportOptions::SAME_TABLES_NOT_REQUIRED,
            // phpcs:ignore
            'expectedSql' => 'CREATE TABLE [destination].[dest] WITH (DISTRIBUTION = ROUND_ROBIN,HEAP) AS SELECT a.[pk1], a.[pk2], a.[col1], a.[col2] FROM (SELECT CAST([pk1] as NVARCHAR(4000)) AS [pk1], CAST([pk2] as NVARCHAR(4000)) AS [pk2], CAST([col1] as NVARCHAR(4000)) AS [col1], CAST([col2] as NVARCHAR(4000)) AS [col2] FROM [source].[source]) AS a',
        ];

        $pk1 = new SynapseColumn('pk1', new Synapse(
            Synapse::TYPE_BIGINT
        ));
        $pk2 = new SynapseColumn('pk2', new Synapse(
            Synapse::TYPE_NUMERIC
        ));
        $col1 = new SynapseColumn('col1', new Synapse(
            Synapse::TYPE_VARCHAR
        ));

        yield 'typed no casting (typed tables with types)' => [
            'sourceColumns' => [
                $pk1,
                $pk2,
                $col1,
                SynapseColumn::createGenericColumn('col2'),
            ],
            'expectedDestination' => $this->getTableDefinition(
                $this->getDestinationSchema(),
                'dest',
                [
                    $pk1,
                    $pk2,
                    $col1,
                    SynapseColumn::createGenericColumn('col2'),
                ]
            ),
            'requireSameTables' => SynapseImportOptions::SAME_TABLES_REQUIRED,
            // phpcs:ignore
            'expectedSql' => 'CREATE TABLE [destination].[dest] WITH (DISTRIBUTION = ROUND_ROBIN,HEAP) AS SELECT [pk1], [pk2], [col1], [col2] FROM [source].[source]',
        ];

        yield 'typed casting (not typed tables with types)' => [
            'sourceColumns' => [
                $pk1,
                $pk2,
                $col1,
                SynapseColumn::createGenericColumn('col2'),
            ],
            'expectedDestination' => $this->getTableDefinition(
                $this->getDestinationSchema(),
                'dest',
                $this->getDefaultColumns()
            ),
            'requireSameTables' => SynapseImportOptions::SAME_TABLES_NOT_REQUIRED,
            // phpcs:ignore
            'expectedSql' => 'CREATE TABLE [destination].[dest] WITH (DISTRIBUTION = ROUND_ROBIN,HEAP) AS SELECT a.[pk1], a.[pk2], a.[col1], a.[col2] FROM (SELECT CAST([pk1] as NVARCHAR(4000)) AS [pk1], CAST([pk2] as NVARCHAR(4000)) AS [pk2], CAST([col1] as NVARCHAR(4000)) AS [col1], CAST([col2] as NVARCHAR(4000)) AS [col2] FROM [source].[source]) AS a',
        ];
    }

    /**
     * @dataProvider ctasProvider
     * @param SynapseColumn[] $sourceColumns
     */
    public function testGetCTASCommandTableSource(
        array $sourceColumns,
        SynapseTableDefinition $expectedDest,
        bool $requireSameTables,
        string $expectedSql
    ): void {
        $source = $this->getTableDefinition($this->getSourceSchema(), 'source', $sourceColumns);
        $this->connection->executeStatement(
            (new SynapseTableQueryBuilder())->getCreateTableCommandFromDefinition($source)
        );
        $this->insertData($this->getSourceSchema(), 'source');
        $sql = FromTableCTASAdapterSqlBuilder::getCTASCommand(
            $expectedDest,
            new Table(
                $source->getSchemaName(),
                $source->getTableName(),
                $source->getColumnsNames()
            ),
            $this->getImportOptions($requireSameTables)
        );
        $this->assertSame($expectedSql, $sql);
        $this->connection->executeStatement($sql);
        $destRef = new SynapseTableReflection(
            $this->connection,
            $expectedDest->getSchemaName(),
            $expectedDest->getTableName()
        );
        $this->assertSame(3, $destRef->getRowsCount());
        Assert::assertSameColumns(
            $destRef->getColumnsDefinitions(),
            $expectedDest->getColumnsDefinitions()
        );
    }

    private function getImportOptions(bool $requireSameTables): SynapseImportOptions
    {
        return new SynapseImportOptions(
            [],
            false,
            false,
            0,
            SynapseImportOptions::CREDENTIALS_SAS,
            SynapseImportOptions::TEMP_TABLE_HEAP,
            SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
            SynapseImportOptions::TABLE_TYPES_PRESERVE,
            $requireSameTables
        );
    }

    /**
     * @return SynapseColumn[]
     */
    private function getDefaultColumns(): array
    {
        return [
            SynapseColumn::createGenericColumn('pk1'),
            SynapseColumn::createGenericColumn('pk2'),
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ];
    }
}
