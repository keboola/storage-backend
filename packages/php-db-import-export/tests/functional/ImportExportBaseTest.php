<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;

abstract class ImportExportBaseTest extends TestCase
{
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
