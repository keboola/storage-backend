<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery\ToFinal;

use DateTimeImmutable;
use DateTimeZone;
use Generator;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\TimestampMode;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Bigquery\BigqueryBaseTestCase;

class IncrementalImportTest extends BigqueryBaseTestCase
{
    protected function getBigqueryIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): BigqueryImportOptions {
        return new BigqueryImportOptions(
            [],
            true,
            true,
            $skipLines,
            BigqueryImportOptions::USING_TYPES_STRING,
        );
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function incrementalImportData(): Generator
    {
        $accountsStub = $this->getParseCsvStub('expectation.tw_accounts.increment.csv');
        $accountsNoDedupStub = $this->getParseCsvStub('expectation.tw_accounts.increment.nodedup.csv');
        $multiPKStub = $this->getParseCsvStub('expectation.multi-pk_not-null.increment.csv');

        yield 'simple no dedup' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            $this->getSimpleImportOptions(),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            $this->getBigqueryIncrementalImportOptions(),
            [$this->getDestinationDbName(), 'accounts-3'],
            $accountsNoDedupStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_3,
            [],
        ];
        yield 'simple' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            $this->getSimpleImportOptions(),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            $this->getBigqueryIncrementalImportOptions(),
            [$this->getDestinationDbName(), 'accounts-3'],
            $accountsStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_3,
            ['id'],
        ];
        yield 'simple no timestamp' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            new BigqueryImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id'],
            ),
            new BigqueryImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_WITHOUT_TS],
            $accountsStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_WITHOUT_TS,
            ['id'],
        ];
        yield 'multi pk' => [
            $this->getSourceInstance(
                'multi-pk_not-null.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            $this->getSimpleImportOptions(),
            $this->getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPKStub->getColumns(),
                false,
                false,
                ['VisitID', 'Value', 'MenuItem'],
            ),
            $this->getBigqueryIncrementalImportOptions(),
            [$this->getDestinationDbName(), self::TABLE_MULTI_PK_WITH_TS],
            $multiPKStub->getRows(),
            3,
            self::TABLE_MULTI_PK_WITH_TS,
            ['VisitID', 'Value', 'MenuItem'],
        ];
    }

    /**
     * @dataProvider  incrementalImportData
     * @param string[] $table
     * @param string[] $dedupCols
     * @param array<mixed> $expected
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        BigqueryImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        BigqueryImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit,
        array $dedupCols,
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var BigqueryTableDefinition $destination */
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $schemaName,
            $tableName,
        ))->getTableDefinition();
        // update PK
        $destination = $this->cloneDefinitionWithDedupCol($destination, $dedupCols);

        $toStageImporter = new ToStageImporter($this->bqClient);
        $fullImporter = new FullImporter($this->bqClient);
        $incrementalImporter = new IncrementalImporter($this->bqClient);

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
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery($this->bqClient->query(
                $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable),
            ));

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
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery($this->bqClient->query(
                $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable),
            ));
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
            $this->bqClient->runQuery($this->bqClient->query(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $fullLoadStagingTable->getSchemaName(),
                    $fullLoadStagingTable->getTableName(),
                ),
            ));
            $this->bqClient->runQuery($this->bqClient->query(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $incrementalLoadStagingTable->getSchemaName(),
                    $incrementalLoadStagingTable->getTableName(),
                ),
            ));
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var BigqueryTableDefinition $destination */
        $this->assertBigqueryTableEqualsExpected(
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
                    '_timestamp'=> '2021-01-01 00:00:00', // no change, no timestamp update
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
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50),
              `price` DECIMAL,
              `isDeleted` INT64,
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        )));
        $this->bqClient->dataset($this->getDestinationDbName())->table(self::TABLE_TRANSLATIONS)->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => 'id',
                    ],
                ],
            ],
        );
        $this->initTable(self::TABLE_TRANSLATIONS, $this->getSourceDbName());
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            self::TABLE_TRANSLATIONS,
        ))->getTableDefinition();

        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `price`, `isDeleted`) VALUES
(1, 'change', 100, 0),
(3, 'test3', 300, 0),
(4, 'test4', 400, 0)
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        )));
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `price`, `isDeleted`, `_timestamp`) VALUES
(1, 'test', 100, 0, '2021-01-01 00:00:00'),
(2, 'test2', 200, 0, '2021-01-01 00:00:00'),
(3, 'test3', 300, 0, '2021-01-01 00:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier(self::TABLE_TRANSLATIONS),
        )));

        $state = new ImportState($destination->getTableName());
        (new IncrementalImporter(
            $this->bqClient,
            new DateTimeImmutable('2022-02-02 00:00:00', new DateTimeZone('UTC')),
        )
        )->importToTable(
            $source,
            $destination,
            new BigqueryImportOptions(
                isIncremental: true,
                useTimestamp: true,
                usingTypes: BigqueryImportOptions::USING_TYPES_USER,
                features: $features,
            ),
            $state,
        );

        $destinationContent = $this->fetchTable($this->getDestinationDbName(), self::TABLE_TRANSLATIONS);
        $this->assertEqualsCanonicalizing($expectedContent, $destinationContent);
    }

    /**
     * Test documenting non-deterministic deduplication behavior with duplicate primary keys.
     *
     * KNOWN LIMITATION: When source table contains multiple rows with identical PK values,
     * the deduplication uses ORDER BY on PK columns only, which provides no tie-breaker.
     * This results in non-deterministic selection of which duplicate row is kept.
     *
     * This test verifies that:
     * 1. Deduplication does occur (only unique PK rows remain)
     * 2. The behavior is currently non-deterministic (may pick different rows on different runs)
     *
     * To fix this issue, the ORDER BY clause in getDedupSelect() should include all columns,
     * not just primary key columns.
     */
    public function testDeduplicationWithDuplicatePKsIsNonDeterministic(): void
    {
        $tableName = 'test_dedup_nondeterministic';

        // Create destination table with PK
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50) NOT NULL,
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));
        $this->bqClient->dataset($this->getDestinationDbName())->table($tableName)->update(
            [
                'tableConstraints' => [
                    'primaryKey' => [
                        'columns' => ['id', 'name'],
                    ],
                ],
            ],
        );

        // Create source table with duplicate PK rows
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64,
              `name` STRING(50),
              `value` STRING(100)
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        // Insert data with duplicate PKs - different non-PK values
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`) VALUES
(1, 'Alice', 'value1'),
(2, 'Bob', 'value2'),
(1, 'Alice', 'value1_duplicate'),
(2, 'Bob', 'value2_duplicate')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            $tableName,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            $tableName,
        ))->getTableDefinition();

        $importOptions = new BigqueryImportOptions(
            isIncremental: true,
            useTimestamp: true,
            usingTypes: BigqueryImportOptions::USING_TYPES_USER,
        );

        $state = new ImportState($destination->getTableName());
        $result = (new IncrementalImporter($this->bqClient))->importToTable(
            $source,
            $destination,
            $importOptions,
            $state,
        );

        // Verify deduplication occurred - should have exactly 2 rows (one per unique PK)
        $destinationData = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT `id`, `name`, `value` FROM %s.%s ORDER BY `id`, `name`',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        $rows = [];
        foreach ($destinationData as $row) {
            assert(is_array($row));
            $rows[] = $row;
        }

        // Verify exactly 2 rows remain (deduplication worked)
        $this->assertCount(2, $rows, 'Deduplication should reduce 4 rows to 2 unique PK rows');

        // Verify correct PK combinations exist
        $this->assertEquals(1, $rows[0]['id']);
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals(2, $rows[1]['id']);
        $this->assertEquals('Bob', $rows[1]['name']);

        // Note: We do NOT assert which 'value' was selected (value1 vs value1_duplicate)
        // because the current implementation is non-deterministic
        $this->assertContains(
            $rows[0]['value'],
            ['value1', 'value1_duplicate'],
            'Value should be one of the duplicate options (non-deterministic selection)',
        );
        $this->assertContains(
            $rows[1]['value'],
            ['value2', 'value2_duplicate'],
            'Value should be one of the duplicate options (non-deterministic selection)',
        );
    }

    public function testIncrementalLoadWithTimestampFromSource(): void
    {
        $tableName = 'test_timestamp_from_source';

        // 1. Create destination table with _timestamp column (typed table)
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64 NOT NULL,
              `name` STRING(50),
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));
        $this->bqClient->dataset($this->getDestinationDbName())->table($tableName)->update([
            'tableConstraints' => [
                'primaryKey' => ['columns' => 'id'],
            ],
        ]);

        // 2. Pre-populate destination with initial data
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`, `_timestamp`) VALUES
(1, 'row1', 'old1', '2020-01-01 00:00:00'),
(2, 'row2', 'old2', '2020-01-01 00:00:00'),
(3, 'row3', 'old3', '2020-01-01 00:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        // 3. Create source table WITH _timestamp column
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'CREATE TABLE %s.%s
            (
              `id` INT64,
              `name` STRING(50),
              `value` STRING(100),
              `_timestamp` TIMESTAMP
           )',
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        // 4. Populate source with explicit timestamps (NOT current time)
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`, `_timestamp`) VALUES
(1, 'row1', 'new1', '2023-06-15 12:00:00'),
(3, 'row3', 'old3', '2023-06-15 12:00:00'),
(4, 'row4', 'val4', '2023-06-15 12:00:00')
SQL,
            BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
            BigqueryQuote::quoteSingleIdentifier($tableName),
        )));

        // 5. Get table definitions
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            $tableName,
        ))->getTableDefinition();
        $source = (new BigqueryTableReflection(
            $this->bqClient,
            $this->getSourceDbName(),
            $tableName,
        ))->getTableDefinition();

        // 6. Run IncrementalImporter with TimestampMode::FromSource
        $state = new ImportState($destination->getTableName());
        (new IncrementalImporter($this->bqClient))->importToTable(
            $source,
            $destination,
            new BigqueryImportOptions(
                isIncremental: true,
                useTimestamp: false,
                usingTypes: BigqueryImportOptions::USING_TYPES_USER,
                timestampMode: TimestampMode::FromSource,
            ),
            $state,
        );

        // 7. Verify results
        // Row 1: data changed (old1 -> new1), timestamp from source
        // Row 2: not in source, keeps original timestamp
        // Row 3: data unchanged, but timestamp column IS compared (USING_TYPES_USER compares all columns),
        //        so row is updated with source timestamp
        // Row 4: new row, inserted with source timestamp
        $expectedContent = [
            ['id' => 1, 'name' => 'row1', 'value' => 'new1', '_timestamp' => '2023-06-15 12:00:00'],
            ['id' => 2, 'name' => 'row2', 'value' => 'old2', '_timestamp' => '2020-01-01 00:00:00'],
            ['id' => 3, 'name' => 'row3', 'value' => 'old3', '_timestamp' => '2023-06-15 12:00:00'],
            ['id' => 4, 'name' => 'row4', 'value' => 'val4', '_timestamp' => '2023-06-15 12:00:00'],
        ];

        $destinationContent = $this->fetchTable($this->getDestinationDbName(), $tableName);
        $this->assertEqualsCanonicalizing($expectedContent, $destinationContent);
    }
}
