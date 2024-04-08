<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit;

use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;

class ImportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ImportOptions();

        self::assertSame([], $options->getConvertEmptyValuesToNull());
        self::assertSame([], $options->features());
        self::assertSame([], $options->ignoreColumns());
        self::assertSame([''], $options->importAsNull());
        self::assertFalse($options->usingUserDefinedTypes());
        self::assertFalse($options->isIncremental());
        self::assertFalse($options->useTimestamp());
        self::assertEquals(0, $options->getNumberOfIgnoredLines());
    }
}
