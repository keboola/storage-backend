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
            $this->createABSSourceInstance('tw_accounts.csv', $accountColumns, false),
            $this->getSimpleImportOptions(),
            $this->createABSSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            $this->getSimpleIncrementalImportOptions(),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $expectedAccountsRows,
            4,
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv', $accountColumns, false),
            new ImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->createABSSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            new ImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-bez-ts'),
            $expectedAccountsRows,
            4,
        ];
        $tests[] = [
            $this->createABSSourceInstance('multi-pk.csv', $multiPkColumns, false),
            $this->getSimpleImportOptions(),
            $this->createABSSourceInstance('multi-pk.increment.csv', $multiPkColumns, false),
            $this->getSimpleIncrementalImportOptions(),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'multi-pk'),
            $expectedMultiPkRows,
            3,
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param Storage\Snowflake\Table $destination
     */
    public function testIncrementalImport(
        Storage\SourceInterface $initialSource,
        ImportOptions $initialOptions,
        Storage\SourceInterface $incrementalSource,
        ImportOptions $incrementalOptions,
        Storage\DestinationInterface $destination,
        array $expected,
        int $expectedImportedRowCount
    ): void {
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
