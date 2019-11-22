<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use PHPUnit\Framework\TestCase;
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
}
