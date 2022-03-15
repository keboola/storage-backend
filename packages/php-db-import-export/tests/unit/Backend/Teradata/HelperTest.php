<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Teradata;

use Keboola\Db\ImportExport\Backend\Teradata\Helper\BackendHelper;
use PHPUnit\Framework\TestCase;

class HelperTest extends TestCase
{
    /**
     * @dataProvider dataProvider
     */
    public function testGetResult($expected, $entries): void
    {
        $this->assertEquals($expected, BackendHelper::getMask($entries));
    }

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
