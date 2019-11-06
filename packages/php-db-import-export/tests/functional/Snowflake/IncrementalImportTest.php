<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class IncrementalImportTest extends SnowflakeImportExportBaseTest
{
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
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $expectedAccountsRows,
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
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-bez-ts'),
            $expectedAccountsRows,
        ];
        $tests[] = [
            $this->createABSSourceInstance('multi-pk.csv', false),
            $this->getSimpleImportOptions($multiPkColumns),
            $this->createABSSourceInstance('multi-pk.increment.csv', false),
            $this->getSimpleIncrementalImportOptions($multiPkColumns),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'multi-pk'),
            $expectedMultiPkRows,
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param Storage\Snowflake\Table $destination
     * @param int|string $sortKey
     */
    public function testIncrementalImport(
        Storage\SourceInterface $initialSource,
        ImportOptions $initialOptions,
        Storage\SourceInterface $incrementalsource,
        ImportOptions $incrementalOptions,
        Storage\DestinationInterface $destination,
        array $expected = [],
        $sortKey = 0
    ): void {
        (new Importer($this->connection))->importTable(
            $initialSource,
            $destination,
            $initialOptions
        );

        (new Importer($this->connection))->importTable(
            $incrementalsource,
            $destination,
            $incrementalOptions
        );

        $this->assertTableEqualsExpected(
            $destination,
            $incrementalOptions,
            $expected,
            $sortKey
        );
    }
}
