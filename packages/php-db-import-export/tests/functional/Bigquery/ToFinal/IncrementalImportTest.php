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
     * Test that deduplication with duplicate primary keys is deterministic.
     * When source table contains multiple rows with identical PK values,
     * the deduplication should consistently choose the same row.
     */
    public function testDeterministicDeduplicationWithDuplicatePrimaryKeys(): void
    {
        $tableName = 'test_dedup_deterministic';

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

        // Insert data with duplicate PKs (id=1,name=Alice appears twice, id=2,name=Bob appears twice)
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            <<<SQL
INSERT INTO %s.%s (`id`, `name`, `value`) VALUES
(1, 'Alice', 'value1'),
(2, 'Bob', 'value2'),
(1, 'Alice', 'value1_updated'),
(2, 'Bob', 'value2_updated')
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

        // Run import multiple times to verify deterministic behavior
        $results = [];
        for ($i = 0; $i < 3; $i++) {
            // Truncate destination before each import
            $this->bqClient->runQuery($this->bqClient->query(sprintf(
                'TRUNCATE TABLE %s.%s',
                BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            )));

            $state = new ImportState($destination->getTableName());
            (new IncrementalImporter($this->bqClient))->importToTable(
                $source,
                $destination,
                $importOptions,
                $state,
            );

            // Fetch results (without timestamp for comparison)
            $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
                'SELECT `id`, `name`, `value` FROM %s.%s ORDER BY `id`, `name`',
                BigqueryQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                BigqueryQuote::quoteSingleIdentifier($tableName),
            )));

            $rows = [];
            foreach ($result as $row) {
                assert(is_array($row));
                $rows[] = [
                    'id' => $row['id'],
                    'name' => $row['name'],
                    'value' => $row['value'],
                ];
            }
            $results[] = $rows;
        }

        // Verify all three runs produced identical results (deterministic)
        $this->assertEquals($results[0], $results[1], 'First and second import runs should produce identical results');
        $this->assertEquals($results[1], $results[2], 'Second and third import runs should produce identical results');

        // Verify we got exactly 2 rows (one for each unique PK combination)
        $this->assertCount(2, $results[0], 'Should have exactly 2 rows after deduplication');

        // Verify the results are deterministically chosen (ordered by all columns)
        $this->assertEquals(1, $results[0][0]['id']);
        $this->assertEquals('Alice', $results[0][0]['name']);
        $this->assertEquals(2, $results[0][1]['id']);
        $this->assertEquals('Bob', $results[0][1]['name']);
    }
}
