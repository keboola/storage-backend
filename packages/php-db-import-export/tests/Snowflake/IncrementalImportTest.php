<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;

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
            $this->getSimpleImportOptions('accounts-3', $accountColumns),
            $this->getSimpleIncrementalImportOptions('accounts-3', $accountColumns),
            $this->createABSSourceInstance('tw_accounts.csv', false),
            $this->createABSSourceInstance('tw_accounts.increment.csv', false),
            $expectedAccountsRows,
        ];
        $tests[] = [
            new ImportOptions(
                self::SNOWFLAKE_DEST_SCHEMA_NAME,
                'accounts-bez-ts',
                [],
                $accountColumns,
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            new ImportOptions(
                self::SNOWFLAKE_DEST_SCHEMA_NAME,
                'accounts-bez-ts',
                [],
                $accountColumns,
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->createABSSourceInstance('tw_accounts.csv', false),
            $this->createABSSourceInstance('tw_accounts.increment.csv', false),
            $expectedAccountsRows,
        ];
        $tests[] = [
            $this->getSimpleImportOptions('multi-pk', $multiPkColumns),
            $this->getSimpleIncrementalImportOptions('multi-pk', $multiPkColumns),
            $this->createABSSourceInstance('multi-pk.csv', false),
            $this->createABSSourceInstance('multi-pk.increment.csv', false),
            $expectedMultiPkRows,
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param int|string $sortKey
     */
    public function testIncrementalImport(
        ImportOptions $initialOptions,
        ImportOptions $incrementalOptions,
        SourceStorage\SourceInterface $initialSource,
        SourceStorage\SourceInterface $incrementalsource,
        array $expected = [],
        $sortKey = 0
    ): void {
        (new Importer($this->connection))->importTable(
            $initialOptions,
            $initialSource
        );

        (new Importer($this->connection))->importTable(
            $incrementalOptions,
            $incrementalsource
        );

        $this->assertTableEqualsExpected(
            $incrementalOptions,
            $expected,
            $sortKey
        );
    }
}
