<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Generator;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

/**
 * This test is limited copy of @FullImportTest with with force ctas.
 */
class CTASFullImportTest extends SnowflakeBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    public function testLoadToFinalTableWithoutDedup(): void
    {
        $this->initTable(self::TABLE_COLUMN_NAME_ROW_NUMBER);

        // skipping header
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: 1,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
        $source = $this->getSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ],
            false,
            false,
            [],
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER,
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options,
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->tmpForceUseCtas();
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState,
        );

        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: false,
            numberOfIgnoredLines: 1,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
        $source = $this->getSourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID'],
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_SINGLE_PK,
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options,
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->tmpForceUseCtas();
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState,
        );

        self::assertEquals(6, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithMultiPK(): void
    {
        $this->initTable(self::TABLE_MULTI_PK);

        // skipping header
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: false,
            numberOfIgnoredLines: 1,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
        $source = $this->getSourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID', 'Something'],
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_MULTI_PK,
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options,
        );

        // now 6 lines. Add one with same VisitId and Something as an existing line has
        // -> expecting that this line will be skipped when DEDUP
        $this->connection->executeQuery(
            sprintf(
                "INSERT INTO %s.%s VALUES ('134', 'xx', 'yy', 'abc', 'def');",
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getTableName()),
            ),
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->tmpForceUseCtas();
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState,
        );

        self::assertEquals(7, $destinationRef->getRowsCount());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function fullImportData(): Generator
    {
        $escapingStub = $this->getParseCsvStub('escaping/standard-with-enclosures.csv');

        // copy from table
        yield 'copy from table' => [
            new Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingStub->getColumns()),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: true,
                numberOfIgnoredLines: 1,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            [['a', 'b'], ['c', 'd']],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        yield 'copy from table 2' => [
            new Table(
                $this->getSourceSchemaName(),
                self::TABLE_TYPES,
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ],
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_TYPES,
            ],
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: true,
                numberOfIgnoredLines: 1,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            [['a', '10.5', '0.3', '1']],
            1,
            self::TABLE_TYPES,
        ];
    }

    /**
     * @dataProvider  fullImportData
     * @param string[] $table
     * @param array<mixed> $expected
     */
    public function testFullImportWithDataSet(
        SourceInterface $source,
        array $table,
        SnowflakeImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit,
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var SnowflakeTableDefinition $destination */
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $schemaName,
            $tableName,
        ))->getTableDefinition();

        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames(),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $toStageImporter = new ToStageImporter($this->connection);
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->tmpForceUseCtas();
        try {
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $options,
            );
            $result = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destination,
                $options,
                $importState,
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName(),
                ),
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertSnowflakeTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0,
        );
    }
}
