<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Teradata;

use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use PHPUnit\Framework\TestCase;

class HelperTest extends TestCase
{
    /**
     * @param string[] $entriesData
     * @dataProvider dataProvider
     */
    public function testGetResult(string $expected, array $entriesData): void
    {
        $mock = $this->createMock(SourceFile::class);

        $mock->method('getManifestEntries')
            ->willReturn($entriesData);
        $this->assertEquals($expected, BackendHelper::getMask($mock));
    }

    /**
     * @return array[]
     */
    public function dataProvider(): array
    {
        return [
            [
                'sliced.csv_*',
                [
                    'sliced.csv_1001',
                    'sliced.csv_0',
                    'sliced.csv_1',
                    'sliced.csv_10',
                    'sliced.csv_100',
                    'sliced.csv_1002',
                ],
            ],
            [
                'sliced0****',
                [
                    'sliced0122.csv',
                    'sliced012.csv',
                    'sliced01.csv',
                    'sliced01999.csv',
                    'sliced0.csv',
                    'sliced034.csv',
                ],
            ],
        ];
    }
}
