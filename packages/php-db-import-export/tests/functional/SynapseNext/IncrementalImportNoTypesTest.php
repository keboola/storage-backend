<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\SynapseNext;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class IncrementalImportNoTypesTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
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
        $expectationMultiPkFile = new CsvFile(self::DATA_DIR . 'expectation.multi-pk.increment.csv');
        $expectedMultiPkRows = [];
        foreach ($expectationMultiPkFile as $row) {
            $expectedMultiPkRows[] = $row;
        }
        $multiPkColumns = array_shift($expectedMultiPkRows);
        $expectedMultiPkRows = array_values($expectedMultiPkRows);

        $tests = [];
        yield 'simple' => [
            $this->createABSSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getSynapseIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'accounts-3'],
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_3],
        ];
        yield 'simple no timestamp' => [
            $this->createABSSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new SynapseImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                // @phpstan-ignore-next-line
                getenv('CREDENTIALS_IMPORT_TYPE'),
                // @phpstan-ignore-next-line
                getenv('TEMP_TABLE_TYPE'),
                // @phpstan-ignore-next-line
                getenv('DEDUP_TYPE')
            ),
            $this->createABSSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new SynapseImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                // @phpstan-ignore-next-line
                getenv('CREDENTIALS_IMPORT_TYPE'),
                // @phpstan-ignore-next-line
                getenv('TEMP_TABLE_TYPE'),
                // @phpstan-ignore-next-line
                getenv('DEDUP_TYPE')
            ),
            [$this->getDestinationSchemaName(), 'accounts-bez-ts'],
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_BEZ_TS],
        ];
        yield 'multi pk' => [
            $this->createABSSourceInstance(
                'multi-pk.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance(
                'multi-pk.increment.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSynapseIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'multi-pk'],
            $expectedMultiPkRows,
            3,
            [self::TABLE_MULTI_PK],
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param array<string,string> $table
     * @param array<mixed> $expected
     * @param string[] $tablesToInit
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        SynapseImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        SynapseImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        array $tablesToInit
    ): void {
        $this->initTables($tablesToInit);

        [$schemaName, $tableName] = $table;
        $destination = (new SynapseTableReflection(
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
            $qb = new SynapseTableQueryBuilder();
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
            $qb = new SynapseTableQueryBuilder();
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

        $this->assertSynapseTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
