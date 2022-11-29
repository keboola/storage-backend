<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata\ToFinal;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportFunctional\Teradata\TeradataBaseTestCase;

class IncrementalImportTest extends TeradataBaseTestCase
{
    use StorageTrait;

    protected function getTeradataIncrementalImportOptions(
        int $skipLines = ImportOptionsInterface::SKIP_FIRST_LINE
    ): TeradataImportOptions {
        return $this->getImportOptions(
            [],
            true,
            true,
            $skipLines
        );
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationSchemaName());
        $this->cleanDatabase($this->getSourceSchemaName());
        $this->createDatabase($this->getSourceSchemaName());
        $this->createDatabase($this->getDestinationSchemaName());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function incrementalImportData(): Generator
    {
        // accounts
        $expectationAccountsFile = new CsvFile(self::DATA_DIR . 'expectation.tw_accounts.increment.csv');
        $expectedAccountsRows = [];
        foreach ($expectationAccountsFile as $row) {
            $expectedAccountsRows[] = $row;
        }
        /** @var string[] $accountColumns */
        $accountColumns = array_shift($expectedAccountsRows);
        $expectedAccountsRows = array_values($expectedAccountsRows);

        // multi pk
        $expectationMultiPkFile = new CsvFile(self::DATA_DIR . 'expectation.multi-pk_not-null.increment.csv');
        $expectedMultiPkRows = [];
        foreach ($expectationMultiPkFile as $row) {
            $expectedMultiPkRows[] = $row;
        }
        /** @var string[] $multiPkColumns */
        $multiPkColumns = array_shift($expectedMultiPkRows);
        $expectedMultiPkRows = array_values($expectedMultiPkRows);

        // multi pk nulls
        $expectationMultiPkNullFile = new CsvFile(self::DATA_DIR . 'expectation.multi-pk.increment.csv');
        $expectedMultiPkNullRows = [];
        foreach ($expectationMultiPkNullFile as $row) {
            $expectedMultiPkNullRows[] = $row;
        }
        /** @var string[] $multiPkColumns */
        $multiPkColumns = array_shift($expectedMultiPkNullRows);
        $expectedMultiPkNullRows = array_values($expectedMultiPkNullRows);

        $multiPkExpectationsWithoutPKFile =  new CsvFile(self::DATA_DIR . 'multi-pk.csv');
        $multiPkExpectationsWithoutPKRows = [];
        foreach ($multiPkExpectationsWithoutPKFile as $row) {
            $multiPkExpectationsWithoutPKRows[] = $row;
        }
        // skip columnNames
        array_shift($multiPkExpectationsWithoutPKRows);

        $tests = [];
        yield 'simple' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getImportOptions([], false, false, ImportOptions::SKIP_FIRST_LINE),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getTeradataIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $expectedAccountsRows,
            4,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'simple no timestamp' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getImportOptions(
                [],
                true,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_WITHOUT_TS],
            $expectedAccountsRows,
            4,
            self::TABLE_ACCOUNTS_WITHOUT_TS,
        ];
        yield 'multi pk' => [
            $this->getSourceInstance(
                'multi-pk_not-null.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getImportOptions(
                [],
                false,
                true, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->getSourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getTeradataIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_MULTI_PK_WITH_TS],
            $expectedMultiPkRows,
            3,
            self::TABLE_MULTI_PK_WITH_TS,
        ];
        yield 'multi pk with null' => [
            $this->getSourceInstance(
                'multi-pk.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getImportOptions(
                [],
                false,
                true, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->getSourceInstance(
                'multi-pk.increment.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getTeradataIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_MULTI_PK_WITH_TS],
            $expectedMultiPkNullRows,
            4,
            self::TABLE_MULTI_PK_WITH_TS,
        ];
        yield 'no pk' => [
            $this->getSourceInstance(
                'multi-pk.csv',
                $multiPkColumns,
                false,
                false,
                []
            ),
            $this->getImportOptions([], false, false, 1),
            $this->getSourceInstance(
                'multi-pk.csv',
                $multiPkColumns,
                false,
                false,
                []
            ),
            $this->getTeradataIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_NO_PK],
            array_merge($multiPkExpectationsWithoutPKRows, $multiPkExpectationsWithoutPKRows),
            6,
            self::TABLE_NO_PK,
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
        TeradataImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        TeradataImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        [$dbName, $tableName] = $table;

        $this->initTable($tablesToInit, $dbName);

        /** @var TeradataTableDefinition $destination */
        $destination = (new TeradataTableReflection(
            $this->connection,
            $dbName,
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
            $qb = new TeradataTableQueryBuilder();
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
            $qb = new TeradataTableQueryBuilder();
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
            $this->dropTableIfExists(
                $fullLoadStagingTable->getSchemaName(),
                $fullLoadStagingTable->getTableName()
            );

            $this->dropTableIfExists(
                $incrementalLoadStagingTable->getSchemaName(),
                $incrementalLoadStagingTable->getTableName()
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var TeradataTableDefinition $destination */
        $this->assertTeradataTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
