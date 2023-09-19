<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class IncrementalImportTest extends SnowflakeBaseTestCase
{
    protected function getSnowflakeIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): SnowflakeImportOptions {
        return new SnowflakeImportOptions(
            [],
            true,
            true,
            $skipLines
        );
    }

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
              "id" NUMBER,
              "VARIANT" VARIANT,
              "BINARY" BINARY,
              "VARBINARY" VARBINARY,
              "OBJECT" OBJECT,
              "ARRAY" ARRAY,
              "_timestamp" TIMESTAMP,
               PRIMARY KEY ("id")
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName())
        ));
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY") 
SELECT 1, 
      TO_VARIANT(\'3.14\'),
      TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
      TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
      OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT),
      ARRAY_CONSTRUCT(1, 2, 3, NULL)
;',
            $this->getDestinationSchemaName(),
            'types'
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
SELECT 1, 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL))
;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName()
        ));
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY") 
SELECT 2, 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL))
;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName()
        ));
        $toFinalTableImporter = new IncrementalImporter($this->connection);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            new ImportState($stagingTable->getTableName())
        );

        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function incrementalImportData(): Generator
    {
        $accountsStub = $this->getParseCsvStub('expectation.tw_accounts.increment.csv');
        $multiPKStub = $this->getParseCsvStub('expectation.multi-pk_not-null.increment.csv');
        $multiPKWithNullStub = $this->getParseCsvStub('expectation.multi-pk.increment.csv');

        $tests = [];
        yield 'simple' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            $this->getSnowflakeImportOptions(),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            $this->getSnowflakeIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'accounts_3'],
            $accountsStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'simple no timestamp' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            new SnowflakeImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            new SnowflakeImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            [$this->getDestinationSchemaName(), 'accounts_without_ts'],
            $accountsStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_WITHOUT_TS,
        ];
        yield 'multi pk' => [
            $this->getSourceInstance(
                'multi-pk_not-null.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSnowflakeImportOptions(),
            $this->getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSnowflakeIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'multi_pk_ts'],
            $multiPKStub->getRows(),
            3,
            self::TABLE_MULTI_PK_WITH_TS,
        ];

        yield 'multi pk with null' => [
            $this->getSourceInstance(
                'multi-pk.csv',
                $multiPKWithNullStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            new SnowflakeImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->getSourceInstance(
                'multi-pk.increment.csv',
                $multiPKWithNullStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSnowflakeIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_MULTI_PK_WITH_TS],
            $multiPKWithNullStub->getRows(),
            4,
            self::TABLE_MULTI_PK_WITH_TS,
        ];

        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param string[] $table
     * @param array<mixed> $expected
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        SnowflakeImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        SnowflakeImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        $toStageImporter = new ToStageImporter($this->connection);
        $fullImporter = new FullImporter($this->connection);
        $incrementalImporter = new IncrementalImporter($this->connection);

        $fullLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $fullLoadSource->getColumnsNames()
        );
        $incrementalLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $incrementalSource->getColumnsNames()
        );

        try {
            // full load
            $qb = new SnowflakeTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable)
            );

            $importState = $toStageImporter->importToStagingTable(
                $fullLoadSource,
                $fullLoadStagingTable,
                $fullLoadOptions
            );
            $fullImporter->importToTable(
                $fullLoadStagingTable,
                $destination,
                $fullLoadOptions,
                $importState
            );
            // incremental load
            $qb = new SnowflakeTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable)
            );
            $importState = $toStageImporter->importToStagingTable(
                $incrementalSource,
                $incrementalLoadStagingTable,
                $incrementalOptions
            );
            $result = $incrementalImporter->importToTable(
                $incrementalLoadStagingTable,
                $destination,
                $incrementalOptions,
                $importState
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $fullLoadStagingTable->getSchemaName(),
                    $fullLoadStagingTable->getTableName()
                )
            );
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $incrementalLoadStagingTable->getSchemaName(),
                    $incrementalLoadStagingTable->getTableName()
                )
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var SnowflakeTableDefinition $destination */
        $this->assertSnowflakeTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
