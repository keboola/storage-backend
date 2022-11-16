<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;

abstract class ImportExportBaseTest extends TestCase
{
    public const TABLE_OUT_CSV_2COLS = 'out_csv_2Cols';
    public const TABLE_OUT_LEMMA = 'out_lemma';
    public const TABLE_ACCOUNTS_3 = 'accounts-3';
    public const TABLE_ACCOUNTS_WITHOUT_TS = 'accounts-without-ts';
    public const TABLE_COLUMN_NAME_ROW_NUMBER = 'column-name-row-number';
    public const TABLE_MULTI_PK = 'multi-pk';
    public const TABLE_MULTI_PK_WITH_TS = 'multi-pk_ts';
    public const TABLE_SINGLE_PK = 'single-pk';
    public const TABLE_NO_PK = 'no-pk';
    public const TABLE_OUT_CSV_2COLS_WITHOUT_TS = 'out_csv_2Cols_without_ts';
    public const TABLE_NULLIFY = 'nullify';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out_no_timestamp_table';
    public const TABLE_TABLE = 'table';
    public const TABLE_TYPES = 'types';
    public const TABLE_TRANSLATIONS = 'transactions';

    protected const DATA_DIR = __DIR__ . '/../data/';
    use StorageTrait;

    protected function getDestinationSchema(): string
    {
        return 'destination';
    }

    protected function getSourceSchema(): string
    {
        return 'source';
    }

    /**
     * @param CsvFile[] $expected
     * @param CsvFile[] $actual
     */
    public function assertCsvFilesCountSliced(array $expected, array $actual): void
    {
        $expectedContent = [];
        foreach ($expected as $item) {
            $item = iterator_to_array($item);
            if (empty($item)) {
                continue;
            }
            $expectedContent = array_merge($expectedContent, $item);
        }
        $actualContent = [];
        foreach ($actual as $item) {
            $item = iterator_to_array($item);
            if (empty($item)) {
                continue;
            }
            $actualContent = array_merge($actualContent, $item);
        }
        $this->assertCount(
            count($expectedContent),
            $actualContent,
            'Csv dont have equals count of items.'
        );
    }

    public function assertCsvFilesSame(CsvFile $expected, CsvFile $actual): void
    {
        $this->assertArrayEqualsSorted(
            iterator_to_array($expected),
            iterator_to_array($actual),
            0,
            'Csv files are not same'
        );
    }

    /**
     * @param array<mixed> $expected
     * @param array<mixed> $actual
     * @param int|string $sortKey
     */
    protected function assertArrayEqualsSorted(
        array $expected,
        array $actual,
        $sortKey,
        string $message = ''
    ): void {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] === $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        $this->assertEqualsCanonicalizing($expected, $actual, $message);
    }

    /**
     * @param CsvFile[] $expected
     * @param CsvFile[] $actual
     */
    public function assertCsvFilesSameSliced(array $expected, array $actual): void
    {
        $expectedContent = [];
        foreach ($expected as $item) {
            $item = iterator_to_array($item);
            if (empty($item)) {
                continue;
            }
            $expectedContent = array_merge($expectedContent, $item);
        }
        $actualContent = [];
        foreach ($actual as $item) {
            $item = iterator_to_array($item);
            if (empty($item)) {
                continue;
            }
            $actualContent = array_merge($actualContent, $item);
        }
        $this->assertArrayEqualsSorted(
            $expectedContent,
            $actualContent,
            0,
            'Csv files are not same'
        );
    }

    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ImportOptions {
        return new ImportOptions(
            [],
            false,
            true,
            $skipLines
        );
    }

    protected function getSimpleIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ImportOptions {
        return new ImportOptions(
            [],
            true,
            true,
            $skipLines
        );
    }

    public const EXPECTATION_FILE_DATA_CONVERT_NULLS = true;
    public const EXPECTATION_FILE_DATA_KEEP_AS_IS = false;
    /**
     * @param self::EXPECTATION_FILE_DATA_* $convertNullsString
     * @return array{0:string[], 1:array<string,mixed>}
     */
    protected function getExpectationFileData(
        string $filePathInDataDir,
        bool $convertNullsString = self::EXPECTATION_FILE_DATA_KEEP_AS_IS
    ): array {
        $expectedRows = [];
        $expectationFile = new CsvFile(self::DATA_DIR . $filePathInDataDir);
        foreach ($expectationFile as $row) {
            if ($convertNullsString === self::EXPECTATION_FILE_DATA_CONVERT_NULLS) {
                $row = array_map(static function ($item) {
                    return $item === 'null' ? null : $item; // convert 'null' string to null to compare
                }, $row);
            }
            $expectedRows[] = $row;
        }
        /** @var string[] $expectedColumns */
        $expectedColumns = array_shift($expectedRows);
        /** @var array<string,mixed> $expectedRows */
        $expectedRows = array_values($expectedRows);

        return [
            $expectedColumns,
            $expectedRows,
        ];
    }
}
