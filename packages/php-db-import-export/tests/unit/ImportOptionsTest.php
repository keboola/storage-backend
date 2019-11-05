<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit;

use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;

class ImportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ImportOptions(
            'schema',
            'tableName'
        );

        self::assertEquals('schema', $options->getSchema());
        self::assertEquals('tableName', $options->getTableName());
        self::assertEquals('"schema"."tableName"', $options->getTargetTableWithScheme());
        self::assertSame([], $options->getColumns());
        self::assertSame([], $options->getConvertEmptyValuesToNull());
        self::assertFalse($options->isIncremental());
        self::assertFalse($options->useTimestamp());
        self::assertEquals(0, $options->getNumberOfIgnoredLines());
    }
}
