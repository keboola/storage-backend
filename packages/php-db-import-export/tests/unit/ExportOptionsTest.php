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

        self::assertFalse($options->isCompressed());
        self::assertFalse($options->generateManifest());
    }

    public function testValues(): void
    {
        $options = new ExportOptions(true, ExportOptions::MANIFEST_AUTOGENERATED);

        self::assertTrue($options->isCompressed());
        self::assertTrue($options->generateManifest());
    }
}