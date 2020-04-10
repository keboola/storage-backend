<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;

abstract class ImportExportBaseTest extends TestCase
{
    protected const DATA_DIR = __DIR__ . '/../data/';
    use ABSSourceTrait;

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
}
