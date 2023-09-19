<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class FullImportTest extends SnowflakeBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    /**
     * Test is testing loading of semi-structured data into typed table.
     *
     * We ignore here GEOGRAPHY and GEOMETRY as they act differently when casting from string
     * https://docs.snowflake.com/en/sql-reference/functions/to_geography
     * https://docs.snowflake.com/en/sql-reference/functions/to_geometry
     *
     * This test is not using CSV but inserting data directly into stage table to mimic this behavior
     */
    public function testLoadTypedTableWithCastingValues(): void
    {
        $this->connection->executeQuery(sprintf(
            /** @lang Snowflake */
            'CREATE TABLE %s."types" (
              "id"  NUMBER,
              "VARIANT" VARIANT,
              "BINARY" BINARY,
              "VARBINARY" VARBINARY,
              "OBJECT" OBJECT,
              "ARRAY" ARRAY,
              "_timestamp" TIMESTAMP
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName())
        ));

        // skipping header
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            1,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ['_timestamp'],
            [
                Snowflake::TYPE_VARIANT,
                Snowflake::TYPE_BINARY,
                Snowflake::TYPE_VARBINARY,
                Snowflake::TYPE_OBJECT,
                Snowflake::TYPE_ARRAY,
            ]
        );

        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'types'
        );
        /** @var SnowflakeTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createVarcharStagingTableDefinition(
            $destination->getSchemaName(),
            [
                'id',
                'VARIANT',
                'BINARY',
                'VARBINARY',
                'OBJECT',
                'ARRAY',
            ]
        );

        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY") 
select 1, 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL))
;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName()
        ));
        $toFinalTableImporter = new FullImporter($this->connection);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            new ImportState($stagingTable->getTableName())
        );

        self::assertEquals(1, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithNullValuesShouldPass(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = $this->getSnowflakeImportOptions(1, false);
        $source = $this->getSourceInstance(
            'multi-pk_null.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_SINGLE_PK
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
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(5, $destinationRef->getRowsCount());
    }

    public function testLoadToFinalTableWithoutDedup(): void
    {
        $this->initTable(self::TABLE_COLUMN_NAME_ROW_NUMBER);

        // skipping header
        $options = $this->getSnowflakeImportOptions(1, false);
        $source = $this->getSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = $this->getSnowflakeImportOptions(1, false);
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
            ['VisitID']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_SINGLE_PK
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
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(4, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithMultiPK(): void
    {
        $this->initTable(self::TABLE_MULTI_PK);

        // skipping header
        $options = $this->getSnowflakeImportOptions(1, false);
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
            ['VisitID', 'Something']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_MULTI_PK
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
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // now 6 lines. Add one with same VisitId and Something as an existing line has
        // -> expecting that this line will be skipped when DEDUP
        $this->connection->executeQuery(
            sprintf(
                "INSERT INTO %s.%s VALUES ('134', 'xx', 'yy', 'abc', 'def');",
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(6, $destinationRef->getRowsCount());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function fullImportData(): Generator
    {
        $escapingStub = $this->getParseCsvStub('escaping/standard-with-enclosures.csv');
        $accountsStub = $this->getParseCsvStub('tw_accounts.csv');
        $accountsChangedColumnsOrderStub = $this->getParseCsvStub('tw_accounts.changedColumnsOrder.csv');
        $lemmaStub = $this->getParseCsvStub('lemma.csv');

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        yield 'large manifest' => [
            $this->getSourceInstance(
                'sliced/2cols-large/%MANIFEST_PREFIX%2cols-large.csvmanifest',
                $escapingStub->getColumns(),
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'empty manifest' => [
            $this->getSourceInstance(
                'empty.manifest',
                $escapingStub->getColumns(),
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'lemma' => [
            $this->getSourceInstance(
                'lemma.csv',
                $lemmaStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_LEMMA],
            $this->getSnowflakeImportOptions(),
            $lemmaStub->getRows(),
            5,
            self::TABLE_OUT_LEMMA,
        ];

        yield 'standard with enclosures' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'gzipped standard with enclosure' => [
            $this->getSourceInstance(
                'gzipped-standard-with-enclosures.csv.gz',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'standard with enclosures tabs' => [
            $this->getSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'accounts changedColumnsOrder' => [
            $this->getSourceInstance(
                'tw_accounts.changedColumnsOrder.csv',
                $accountsChangedColumnsOrderStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_ACCOUNTS_3,
            ],
            $this->getSnowflakeImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'accounts' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSnowflakeImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // line ending detection is not supported yet for S3
        yield 'accounts crlf' => [
            $this->getSourceInstance(
                'tw_accounts.crlf.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSnowflakeImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // manifests
        yield 'accounts sliced' => [
            $this->getSourceInstance(
                'sliced/accounts/%MANIFEST_PREFIX%accounts.csvmanifest',
                $accountsStub->getColumns(),
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSnowflakeImportOptions(ImportOptions::SKIP_NO_LINE),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts sliced gzip' => [
            $this->getSourceInstance(
                'sliced/accounts-gzip/%MANIFEST_PREFIX%accounts-gzip.csvmanifest',
                $accountsStub->getColumns(),
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSnowflakeImportOptions(ImportOptions::SKIP_NO_LINE),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // folder
        yield 'accounts sliced folder import' => [
            $this->getSourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsStub->getColumns(),
                true,
                true,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSnowflakeImportOptions(ImportOptions::SKIP_NO_LINE),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // reserved words
        yield 'reserved words' => [
            $this->getSourceInstance(
                'reserved-words.csv',
                ['column', 'table'],
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_TABLE],
            $this->getSnowflakeImportOptions(),
            [['table', 'column', null]],
            1,
            self::TABLE_TABLE,
        ];
        // import table with _timestamp columns - used by snapshots
        yield 'import with _timestamp columns' => [
            $this->getSourceInstance(
                'with-ts.csv',
                [
                    'col1',
                    'col2',
                    '_timestamp',
                ],
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_CSV_2COLS,
            ],
            $this->getSnowflakeImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06'],
                ['c', 'd', '2014-11-10 14:12:06'],
            ],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        // test creating table without _timestamp column
        yield 'table without _timestamp column' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE,
            ],
            new SnowflakeImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
        // copy from table
        yield 'copy from table' => [
            new Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingStub->getColumns()),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSnowflakeImportOptions(),
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
                ]
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_TYPES,
            ],
            $this->getSnowflakeImportOptions(),
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
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var SnowflakeTableDefinition $destination */
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $toStageImporter = new ToStageImporter($this->connection);
        $toFinalTableImporter = new FullImporter($this->connection);
        try {
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $options
            );
            $result = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destination,
                $options,
                $importState
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName()
                )
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertSnowflakeTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
