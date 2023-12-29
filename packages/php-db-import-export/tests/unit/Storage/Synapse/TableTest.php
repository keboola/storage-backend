<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Synapse;

use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Synapse\Table('schema', 'table');
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('schema', $source->getSchema());
        self::assertEquals('table', $source->getTableName());
        self::assertEquals([], $source->getQueryBindings());
        self::assertEquals([], $source->getColumnsNames());
        self::assertEquals('SELECT * FROM [schema].[table]', $source->getFromStatement());
        self::assertEquals('SELECT * FROM [schema].[table]', $source->getFromStatementWithStringCasting());
        self::assertEquals('SELECT * FROM [schema].[table]', $source->getFromStatementForStaging(false));
        self::assertEquals('SELECT * FROM [schema].[table]', $source->getFromStatementForStaging(true));
        self::assertNull($source->getPrimaryKeysNames());
    }

    public function testColumns(): void
    {
        $source = new Storage\Synapse\Table('schema', 'table', ['col1', 'col2']);
        self::assertEquals(['col1', 'col2'], $source->getColumnsNames());
        self::assertEquals('SELECT [col1], [col2] FROM [schema].[table]', $source->getFromStatement());
        self::assertEquals('SELECT [col1], [col2] FROM [schema].[table]', $source->getFromStatementWithStringCasting());
        self::assertEquals('SELECT [col1], [col2] FROM [schema].[table]', $source->getFromStatementForStaging(false));
        self::assertEquals(
        // phpcs:ignore
            'SELECT a.[col1], a.[col2] FROM (SELECT CAST([col1] as NVARCHAR(4000)) AS [col1], CAST([col2] as NVARCHAR(4000)) AS [col2] FROM [schema].[table]) AS a',
            $source->getFromStatementForStaging(true),
        );
    }
}
