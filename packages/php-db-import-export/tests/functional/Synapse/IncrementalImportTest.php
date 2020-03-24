<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class IncrementalImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::SYNAPSE_DEST_SCHEMA_NAME);
        $this->dropAllWithinSchema(self::SYNAPSE_SOURCE_SCHEMA_NAME);
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_DEST_SCHEMA_NAME));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_SOURCE_SCHEMA_NAME));
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
            $this->createABSSourceInstance('tw_accounts.csv', false),
            $this->getSimpleImportOptions($accountColumns),
            $this->createABSSourceInstance('tw_accounts.increment.csv', false),
            $this->getSimpleIncrementalImportOptions($accountColumns),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'accounts-3'),
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_3],
            true,
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv', false),
            new ImportOptions(
                [],
                $accountColumns,
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->createABSSourceInstance('tw_accounts.increment.csv', false),
            new ImportOptions(
                [],
                $accountColumns,
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'accounts-bez-ts'),
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_BEZ_TS],
            true,
        ];
        $tests[] = [
            $this->createABSSourceInstance('multi-pk.csv', false),
            $this->getSimpleImportOptions($multiPkColumns),
            $this->createABSSourceInstance('multi-pk.increment.csv', false),
            $this->getSimpleIncrementalImportOptions($multiPkColumns),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'multi-pk'),
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
        ImportOptions $initialOptions,
        Storage\SourceInterface $incrementalSource,
        ImportOptions $incrementalOptions,
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

        if ($isSkipped === true) {
            $this->markTestIncomplete(
                'Skip assertion due to UTF-8 COPY INTO error in synapse.'
            );
        }

        $this->assertTableEqualsExpected(
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
