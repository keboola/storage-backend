<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Bigquery;

use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table');
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('schema', $source->getSchema());
        self::assertEquals('table', $source->getTableName());
        self::assertEquals([], $source->getQueryBindings());
        self::assertEquals([], $source->getColumnsNames());
        self::assertEquals('SELECT * FROM `schema`.`table`', $source->getFromStatement());
        self::assertEquals('SELECT * FROM `schema`.`table`', $source->getFromStatementWithStringCasting());
        self::assertNull($source->getPrimaryKeysNames());
    }

    public function testColumns(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table', ['col1', 'col2']);
        self::assertEquals(['col1', 'col2'], $source->getColumnsNames());
        self::assertEquals('SELECT `col1`, `col2` FROM `schema`.`table`', $source->getFromStatement());
        self::assertEquals(
            'SELECT CAST(`col1` AS STRING), CAST(`col2` AS STRING) FROM `schema`.`table`',
            $source->getFromStatementWithStringCasting(),
        );
    }
}
