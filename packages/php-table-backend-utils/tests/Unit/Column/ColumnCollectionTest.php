<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
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

    public function testCount(): void
    {
        $collection = new ColumnCollection([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ]);
        $this->assertCount(2, $collection);
    }
}
