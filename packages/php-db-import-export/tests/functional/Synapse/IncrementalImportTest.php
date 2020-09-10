<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Synapse\SynapseImportOptions;

class IncrementalImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function incrementalImportData(): array
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
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv', $accountColumns, false),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            $this->getSynapseIncrementalImportOptions(),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), 'accounts-3'),
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_3],
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv', $accountColumns, false),
            new SynapseImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                getenv('CREDENTIALS_IMPORT_TYPE')
            ),
            $this->createABSSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            new SynapseImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                getenv('CREDENTIALS_IMPORT_TYPE')
            ),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), 'accounts-bez-ts'),
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_BEZ_TS],
        ];
        $tests[] = [
            $this->createABSSourceInstance('multi-pk.csv', $multiPkColumns, false),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance('multi-pk.increment.csv', $multiPkColumns, false),
            $this->getSynapseIncrementalImportOptions(),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), 'multi-pk'),
            $expectedMultiPkRows,
            3,
            [self::TABLE_MULTI_PK],
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param Storage\Synapse\Table $destination
     */
    public function testIncrementalImport(
        Storage\SourceInterface $initialSource,
        SynapseImportOptions $initialOptions,
        Storage\SourceInterface $incrementalSource,
        SynapseImportOptions $incrementalOptions,
        Storage\DestinationInterface $destination,
        array $expected,
        int $expectedImportedRowCount,
        array $tablesToInit,
        bool $isSkipped = false
    ): void {
        $this->initTables($tablesToInit);

        (new Importer($this->connection))->importTable(
            $initialSource,
            $destination,
            $initialOptions
        );

        $result = (new Importer($this->connection))->importTable(
            $incrementalSource,
            $destination,
            $incrementalOptions
        );
        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertTableEqualsExpected(
            $initialSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
