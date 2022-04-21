<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;

class IncrementalImportTest extends ExasolBaseTestCase
{
    use S3SourceTrait;

    protected function getExasolIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ExasolImportOptions {
        return new ExasolImportOptions(
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
     * @return \Generator<string, array<mixed>>
     */
    public function incrementalImportData(): \Generator
    {
        // accounts
        $expectationAccountsFile = new CsvFile(self::DATA_DIR . 'expectation.tw_accounts.increment.csv');
        $expectedAccountsRows = [];
        foreach ($expectationAccountsFile as $row) {
            $expectedAccountsRows[] = $row;
        }
        $accountColumns = array_shift($expectedAccountsRows);
        $expectedAccountsRows = array_values($expectedAccountsRows);

        // multi pk
        $expectationMultiPkFile = new CsvFile(self::DATA_DIR . 'expectation.multi-pk_not-null.increment.csv');
        $expectedMultiPkRows = [];
        foreach ($expectationMultiPkFile as $row) {
            $expectedMultiPkRows[] = $row;
        }
        $multiPkColumns = array_shift($expectedMultiPkRows);
        $expectedMultiPkRows = array_values($expectedMultiPkRows);

        $tests = [];
        yield 'simple' => [
            $this->createS3SourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getExasolImportOptions(),
            $this->createS3SourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getExasolIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'accounts-3'],
            $expectedAccountsRows,
            4,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'simple no timestamp' => [
            $this->createS3SourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new ExasolImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->createS3SourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new ExasolImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            [$this->getDestinationSchemaName(), 'accounts-bez-ts'],
            $expectedAccountsRows,
            4,
            self::TABLE_ACCOUNTS_BEZ_TS,
        ];
        yield 'multi pk' => [
            $this->createS3SourceInstance(
                'multi-pk_not-null.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getExasolImportOptions(),
            $this->createS3SourceInstance(
                'multi-pk_not-null.increment.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getExasolIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'multi-pk_ts'],
            $expectedMultiPkRows,
            3,
            self::TABLE_MULTI_PK_WITH_TS,
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param array<string,string> $table
     * @param array<mixed> $expected
     * @param string $tablesToInit
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        ExasolImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        ExasolImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        $destination = (new ExasolTableReflection(
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
            $qb = new ExasolTableQueryBuilder();
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
            $qb = new ExasolTableQueryBuilder();
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

        /** @var ExasolTableDefinition $destination */
        $this->assertExasolTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
