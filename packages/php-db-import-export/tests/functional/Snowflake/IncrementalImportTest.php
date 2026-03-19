<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\Attributes\DataProvider;

class IncrementalImportTest extends SnowflakeImportExportBaseTest
{
    /**
     * @return array<mixed>
     */
    public static function incrementalImportData(): array
    {
        // accounts
        $expectationAccountsFile = new CsvFile(self::DATA_DIR . 'expectation.tw_accounts.increment.csv');
        $expectedAccountsRows = [];
        foreach ($expectationAccountsFile as $row) {
            $expectedAccountsRows[] = $row;
        }
        $accountColumns = array_shift($expectedAccountsRows);

        // multi pk
        $expectationMultiPkFile = new CsvFile(self::DATA_DIR . 'expectation.multi-pk.increment.csv');
        $expectedMultiPkRows = [];
        foreach ($expectationMultiPkFile as $row) {
            $expectedMultiPkRows[] = $row;
        }
        $multiPkColumns = array_shift($expectedMultiPkRows);

        $tests = [];
        $tests[] = [
            static::getSourceInstance('tw_accounts.csv', $accountColumns, false),
            static::getSimpleImportOptions(),
            static::getSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            static::getSimpleIncrementalImportOptions(),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-3'),
            $expectedAccountsRows,
            4,
        ];
        $tests[] = [
            static::getSourceInstance('tw_accounts.csv', $accountColumns, false),
            new ImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            static::getSourceInstance('tw_accounts.increment.csv', $accountColumns, false),
            new ImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-without-ts'),
            $expectedAccountsRows,
            4,
        ];
        $tests[] = [
            static::getSourceInstance('multi-pk.csv', $multiPkColumns, false),
            static::getSimpleImportOptions(),
            static::getSourceInstance('multi-pk.increment.csv', $multiPkColumns, false),
            static::getSimpleIncrementalImportOptions(),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'multi-pk'),
            $expectedMultiPkRows,
            4,
        ];
        return $tests;
    }

    /**
     * @param Storage\Snowflake\Table $destination
     * @param array<mixed> $expected
     */
    #[DataProvider('incrementalImportData')]
    public function testIncrementalImport(
        Storage\SourceInterface $initialSource,
        ImportOptions $initialOptions,
        Storage\SourceInterface $incrementalSource,
        ImportOptions $incrementalOptions,
        Storage\DestinationInterface $destination,
        array $expected,
        int $expectedImportedRowCount,
    ): void {
        (new Importer($this->connection))->importTable(
            $initialSource,
            $destination,
            $initialOptions,
        );

        $result = (new Importer($this->connection))->importTable(
            $incrementalSource,
            $destination,
            $incrementalOptions,
        );
        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertTableEqualsExpected(
            $initialSource,
            $destination,
            $incrementalOptions,
            $expected,
            0,
        );
    }
}
