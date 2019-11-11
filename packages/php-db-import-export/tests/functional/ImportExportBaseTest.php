<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use Keboola\Csv\CsvFile;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;

abstract class ImportExportBaseTest extends TestCase
{
    protected const DATA_DIR = __DIR__ . '/../data/';

    use ABSSourceTrait;

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
        $this->assertEquals($expected, $actual, $message);
    }

    protected function getCsvFilesForManifest(string $manifest): array
    {
        $path = (new SplFileInfo(self::DATA_DIR . $manifest, '', ''))->getPath();
        $files = (new Finder)->in($path)->files()->depth(0)->name('/^((?!.csvmanifest).)*$/');

        $filesContent = [];
        $filesHeader = [];
        /** @var SplFileInfo $file */
        foreach ($files as $file) {
            $csvFile = new CsvFile($file->getPathname());
            $csvFileRows = [];
            foreach ($csvFile as $row) {
                $csvFileRows[] = $row;
            }

            if (empty($filesHeader)) {
                $filesHeader = array_shift($csvFileRows);
            } else {
                $this->assertSame(
                    $filesHeader,
                    array_shift($csvFileRows),
                    'Provided files have incosistent headers'
                );
            }
            foreach ($csvFileRows as $fileRow) {
                $filesContent[] = $fileRow;
            }
        }

        return [$filesHeader, $filesContent];
    }
}
