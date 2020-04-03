<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\TableBackendUtils\Column\ColumnIterator;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use PHPUnit\Framework\TestCase;

class ColumnIteratorTest extends TestCase
{
    public function testHasColumnWithName(): void
    {
        $iterator = $this->getIterator();

        $this->assertTrue($iterator->hasColumnWithName('col1'));
        $this->assertFalse($iterator->hasColumnWithName('nonExistent'));
    }

    private function getIterator(): ColumnIterator
    {
        return new ColumnIterator([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
            SynapseColumn::createGenericColumn('col3'),
        ]);
    }

    public function testGetColumnsNames(): void
    {
        $iterator = $this->getIterator();
        $names = $iterator->getColumnsNames();
        $this->assertSame(
            [
                'col1',
                'col2',
                'col3',
            ],
            $names
        );
    }

    public function testCurrent(): void
    {
        $iterator = $this->getIterator();
        $current = $iterator->current();

        $this->assertEquals('col1', $current->getColumnName());
    }

    public function testNext(): void
    {
        $iterator = $this->getIterator();
        $iterator->next();

        $this->assertEquals(1, $iterator->key());
    }

    public function testKey(): void
    {
        $iterator = $this->getIterator();

        $iterator->next();
        $iterator->next();

        $this->assertEquals(2, $iterator->key());
    }

    public function testValidIfItemInvalid(): void
    {
        $iterator = $this->getIterator();

        $iterator->next();
        $iterator->next();
        $iterator->next();

        $this->assertEquals(false, $iterator->valid());
    }

    public function testValidIfItemIsValid(): void
    {
        $iterator = $this->getIterator();

        $iterator->next();

        $this->assertEquals(true, $iterator->valid());
    }

    public function testRewind(): void
    {
        $iterator = $this->getIterator();

        $iterator->rewind();

        $this->assertEquals(0, $iterator->key());
    }
}
