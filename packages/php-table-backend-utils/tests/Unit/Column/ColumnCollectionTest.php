<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ColumnException;
use Keboola\TableBackendUtils\QueryBuilderException;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testGetIterator(): void
    {
        $collection = new ColumnCollection([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ]);

        $this->assertIsIterable($collection);
    }

    public function testTooMuchColumns(): void
    {
        $cols = [];
        for ($i = 0; $i < 1026; $i++) {
            $cols[] = SynapseColumn::createGenericColumn('name' . $i);
        }

        $this->expectException(ColumnException::class);
        $this->expectExceptionMessage('Too many columns. Maximum is 1024 columns.');
        new ColumnCollection($cols);
    }

    public function testCount(): void
    {
        $collection = new ColumnCollection([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ]);
        $this->assertCount(2, $collection);
    }
}
