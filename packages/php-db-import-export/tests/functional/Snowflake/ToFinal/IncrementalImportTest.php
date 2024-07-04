<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use DateTimeImmutable;
use DateTimeZone;
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
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
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
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): SnowflakeImportOptions {
        return new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: true,
            useTimestamp: true,
            numberOfIgnoredLines: $skipLines,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
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
              "GEOGRAPHY" GEOGRAPHY,
              "GEOMETRY" GEOMETRY,
              "_timestamp" TIMESTAMP,
               PRIMARY KEY ("id")
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
        ));
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","GEOGRAPHY","GEOMETRY") 
SELECT 1, 
      TO_VARIANT(\'3.14\'),
      TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
      TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
      OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT),
      ARRAY_CONSTRUCT(1, 2, 3, NULL),
      \'POINT(-122.35 37.55)\',
      \'POINT(1820.12 890.56)\'
;',
            $this->getDestinationSchemaName(),
            'types',
        ));

        // skipping header
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: false,
            numberOfIgnoredLines: 1,
            requireSameTables: SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );

        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'types',
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
                'GEOGRAPHY',
                'GEOMETRY',
            ],
        );

        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","GEOGRAPHY","GEOMETRY") 
SELECT 1, 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL)),
       \'POINT(-122.35 37.55)\',
       \'POINT(1820.12 890.56)\'
;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName(),
        ));
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("id","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","GEOGRAPHY","GEOMETRY") 
SELECT 2, 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL)),
       \'POINT(-122.35 37.55)\',
       \'POINT(1820.12 890.56)\'
;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName(),
        ));
        $toFinalTableImporter = new IncrementalImporter($this->connection);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            new ImportState($stagingTable->getTableName()),
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
                ['id'],
            ),
            $this->getSnowflakeImportOptions(),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
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
                ['id'],
            ),
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false, // disable timestamp
                numberOfIgnoredLines: ImportOptions::SKIP_FIRST_LINE,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: true, // incremental
                useTimestamp: false, // disable timestamp
                numberOfIgnoredLines: ImportOptions::SKIP_FIRST_LINE,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
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
                ['VisitID', 'Value', 'MenuItem'],
            ),
            $this->getSnowflakeImportOptions(),
            $this->getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
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
                ['VisitID', 'Value', 'MenuItem'],
            ),
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: true, // incremental
                useTimestamp: false, // disable timestamp
                numberOfIgnoredLines: ImportOptions::SKIP_FIRST_LINE,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            $this->getSourceInstance(
                'multi-pk.increment.csv',
                $multiPKWithNullStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
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
        string $tablesToInit,
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $schemaName,
            $tableName,
        ))->getTableDefinition();

        $toStageImporter = new ToStageImporter($this->connection);
        $fullImporter = new FullImporter($this->connection);
        $incrementalImporter = new IncrementalImporter($this->connection);

        $fullLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $fullLoadSource->getColumnsNames(),
        );
        $incrementalLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $incrementalSource->getColumnsNames(),
        );

        try {
            // full load
            $qb = new SnowflakeTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable),
            );

            $importState = $toStageImporter->importToStagingTable(
                $fullLoadSource,
                $fullLoadStagingTable,
                $fullLoadOptions,
            );
            $fullImporter->importToTable(
                $fullLoadStagingTable,
                $destination,
                $fullLoadOptions,
                $importState,
            );
            // incremental load
            $qb = new SnowflakeTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable),
            );
            $importState = $toStageImporter->importToStagingTable(
                $incrementalSource,
                $incrementalLoadStagingTable,
                $incrementalOptions,
            );
            $result = $incrementalImporter->importToTable(
                $incrementalLoadStagingTable,
                $destination,
                $incrementalOptions,
                $importState,
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $fullLoadStagingTable->getSchemaName(),
                    $fullLoadStagingTable->getTableName(),
                ),
            );
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $incrementalLoadStagingTable->getSchemaName(),
                    $incrementalLoadStagingTable->getTableName(),
                ),
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var SnowflakeTableDefinition $destination */
        $this->assertSnowflakeTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0,
        );
    }


    public function incrementalImportTimestampBehavior(): Generator
    {
        yield 'import typed table, timestamp update always `no feature`' => [
            'features' => [],
            'expectedContent' => [
                [
                    'id'=> 1,
                    'name'=> 'change',
                    'price'=> 100,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
                [
                    'id'=> 2,
                    'name'=> 'test2',
                    'price'=> 200,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00',
                ],
                [
                    'id'=> 3,
                    'name'=> 'test3',
                    'price'=> 300,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00', // no change timestamp updated
                ],
                [
                    'id'=> 4,
                    'name'=> 'test4',
                    'price'=> 400,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
            ],
        ];

        yield 'import typed table, timestamp update onchange feature: `new-native-types`' => [
            'features' => ['new-native-types'],
            'expectedContent' => [
                [
                    'id'=> 1,
                    'name'=> 'change',
                    'price'=> 100,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
                [
                    'id'=> 2,
                    'name'=> 'test2',
                    'price'=> 200,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00',
                ],
                [
                    'id'=> 3,
                    'name'=> 'test3',
                    'price'=> 300,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00', // no change no timestamp update
                ],
                [
                    'id'=> 4,
                    'name'=> 'test4',
                    'price'=> 400,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
            ],
        ];

        yield 'import typed table, timestamp update onchange feature: `native-types_timestamp-bc`' => [
            'features' => ['native-types_timestamp-bc'],
            'expectedContent' => [
                [
                    'id'=> 1,
                    'name'=> 'change',
                    'price'=> 100,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
                [
                    'id'=> 2,
                    'name'=> 'test2',
                    'price'=> 200,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00',
                ],
                [
                    'id'=> 3,
                    'name'=> 'test3',
                    'price'=> 300,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2021-01-01 00:00:00', // no change no timestamp update
                ],
                [
                    'id'=> 4,
                    'name'=> 'test4',
                    'price'=> 400,
                    'isDeleted'=> 0,
                    '_timestamp'=> '2022-02-02 00:00:00',
                ],
            ],
        ];
    }

    /**
     * @dataProvider incrementalImportTimestampBehavior
     * @param string[] $features
     * @param array<mixed> $expectedContent
     */
    public function testImportTimestampBehavior(array $features, array $expectedContent): void
    {
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" INT CONSTRAINT "table_pk" PRIMARY KEY,
    "name" STRING,
    "price" INT,
    "isDeleted" INT,
    "_timestamp" TIMESTAMP
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" INT,
    "name" STRING,
    "price" INT,
    "isDeleted" INT
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeStatement(sprintf(
            'INSERT INTO "%s"."%s" VALUES
                (1, \'change\', 100, 0), (3, \'test3\', 300, 0), (4, \'test4\', 400, 0);
        ',
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $this->connection->executeStatement(sprintf(
            'INSERT INTO "%s"."%s" VALUES
(1, \'test\', 100, 0, \'2021-01-01 00:00:00\'),
(2, \'test2\', 200, 0, \'2021-01-01 00:00:00\'),
(3, \'test3\', 300, 0, \'2021-01-01 00:00:00\')
        ',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $source = (new SnowflakeTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();

        $state = new ImportState(self::TABLE_TRANSLATIONS);
        (new IncrementalImporter(
            $this->connection,
            new DateTimeImmutable('2022-02-02 00:00:00', new DateTimeZone('UTC')),
        )
        )->importToTable(
            $source,
            $destination,
            new SnowflakeImportOptions(
                isIncremental: true,
                useTimestamp: true,
                nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
                features: $features,
            ),
            $state,
        );

        $destinationContent = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
            ),
        );
        $this->assertEqualsCanonicalizing($expectedContent, $destinationContent);
    }

    public function testImportTimestampBehaviorArrayType(): void
    {
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" INT CONSTRAINT "table_pk" PRIMARY KEY,
    "name" STRING,
    "array" ARRAY,
    "object" OBJECT,
    "isDeleted" INT,
    "_timestamp" TIMESTAMP
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" INT,
    "name" STRING,
    "array" ARRAY,
    "object" OBJECT,
    "isDeleted" INT
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT 1, \'test1\', ARRAY_CONSTRUCT(2, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  41::VARIANT), 0;',  //<- changed array value
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT 2, \'test2\', ARRAY_CONSTRUCT(2, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT), 0;',
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT 4, \'test4\', ARRAY_CONSTRUCT(4, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  44::VARIANT), 0;',
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 1, \'test1\', ARRAY_CONSTRUCT(1, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  41::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 2, \'test2\', ARRAY_CONSTRUCT(2, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 3, \'test3\', ARRAY_CONSTRUCT(3, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  43::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $source = (new SnowflakeTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();

        $state = new ImportState(self::TABLE_TRANSLATIONS);
        (new IncrementalImporter(
            $this->connection,
            new DateTimeImmutable('2022-02-02 00:00:00', new DateTimeZone('UTC')),
        )
        )->importToTable(
            $source,
            $destination,
            new SnowflakeImportOptions(
                isIncremental: true,
                useTimestamp: true,
                nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
                features: ['native-types_timestamp-bc'],
            ),
            $state,
        );

        $destinationContent = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
            ),
        );
        $this->assertEqualsCanonicalizing([
            [
                'id'=> 1,
                'name'=> 'test1',
                'array'=> '[
  2,
  undefined
]',
                'object' => '{
  "age": 41,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2022-02-02 00:00:00',
            ],
            [
                'id'=> 2,
                'name'=> 'test2',
                'array'=> '[
  2,
  undefined
]',
                'object' => '{
  "age": 42,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2021-01-01 00:00:00',
            ],
            [
                'id'=> 3,
                'name'=> 'test3',
                'array'=> '[
  3,
  undefined
]',
                'object' => '{
  "age": 43,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2021-01-01 00:00:00', // no change no timestamp update
            ],
            [
                'id'=> 4,
                'name'=> 'test4',
                'array'=> '[
  4,
  undefined
]',
                'object' => '{
  "age": 44,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2022-02-02 00:00:00',
            ],
        ], $destinationContent);
    }


    public function testImportTimestampBehaviorArrayTypeWithStringSourceTable(): void
    {
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" INT CONSTRAINT "table_pk" PRIMARY KEY,
    "name" STRING,
    "array" ARRAY,
    "object" OBJECT,
    "isDeleted" INT,
    "_timestamp" TIMESTAMP
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeQuery(sprintf(
            'CREATE TABLE %s.%s (
    "id" STRING,
    "name" STRING,
    "array" STRING,
    "object" STRING,
    "isDeleted" STRING
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        ));

        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT TO_VARCHAR(1), \'test1\', TO_VARCHAR(ARRAY_CONSTRUCT(2, NULL)), TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  41::VARIANT)), TO_VARCHAR(0);',  //<- changed array value
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT TO_VARCHAR(2), \'test2\', TO_VARCHAR(ARRAY_CONSTRUCT(2, NULL)), TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)), TO_VARCHAR(0);',
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted") 
SELECT TO_VARCHAR(4), \'test4\', TO_VARCHAR(ARRAY_CONSTRUCT(4, NULL)), TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  44::VARIANT)), TO_VARCHAR(0);',
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 1, \'test1\', ARRAY_CONSTRUCT(1, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  41::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 2, \'test2\', ARRAY_CONSTRUCT(2, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));
        $this->connection->executeStatement(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."%s" ("id", "name", "array", "object", "isDeleted", "_timestamp") 
SELECT 3, \'test3\', ARRAY_CONSTRUCT(3, NULL), OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  43::VARIANT), 0, \'2021-01-01 00:00:00\';',
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ));

        $source = (new SnowflakeTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();
        $destination = (new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();

        $state = new ImportState(self::TABLE_TRANSLATIONS);
        (new IncrementalImporter(
            $this->connection,
            new DateTimeImmutable('2022-02-02 00:00:00', new DateTimeZone('UTC')),
        )
        )->importToTable(
            $source,
            $destination,
            new SnowflakeImportOptions(
                isIncremental: true,
                useTimestamp: true,
                nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
                features: ['native-types_timestamp-bc'],
            ),
            $state,
        );

        $destinationContent = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
            ),
        );
        $this->assertEqualsCanonicalizing([
            [
                'id'=> 1,
                'name'=> 'test1',
                'array'=> '[
  2,
  undefined
]',
                'object' => '{
  "age": 41,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2022-02-02 00:00:00',
            ],
            [
                'id'=> 2,
                'name'=> 'test2',
                'array'=> '[
  2,
  undefined
]',
                'object' => '{
  "age": 42,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2021-01-01 00:00:00',
            ],
            [
                'id'=> 3,
                'name'=> 'test3',
                'array'=> '[
  3,
  undefined
]',
                'object' => '{
  "age": 43,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2021-01-01 00:00:00', // no change no timestamp update
            ],
            [
                'id'=> 4,
                'name'=> 'test4',
                'array'=> '[
  4,
  undefined
]',
                'object' => '{
  "age": 44,
  "name": "Jones"
}',
                'isDeleted'=> 0,
                '_timestamp'=> '2022-02-02 00:00:00',
            ],
        ], $destinationContent);
    }
}
