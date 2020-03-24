<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit;

use Keboola\Db\ImportExport\ExportOptions;
use PHPUnit\Framework\TestCase;

class ExportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new ExportOptions();

        self::assertSame(false, $options->isCompressed());
    }
}
