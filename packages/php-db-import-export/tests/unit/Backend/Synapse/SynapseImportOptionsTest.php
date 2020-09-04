<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Synapse\SynapseImportOptions;
use PHPUnit\Framework\TestCase;

class SynapseImportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new SynapseImportOptions();

        self::assertSame([], $options->getConvertEmptyValuesToNull());
        self::assertFalse($options->isIncremental());
        self::assertFalse($options->useTimestamp());
        self::assertEquals(0, $options->getNumberOfIgnoredLines());
        self::assertEquals('SAS', $options->getImportCredentialsType());
    }

    public function testValues(): void
    {
        $options = new SynapseImportOptions(
            ['col1'],
            true,
            true,
            SynapseImportOptions::SKIP_FIRST_LINE,
            SynapseImportOptions::CREDENTIALS_MANAGED_IDENTITY
        );

        self::assertSame(['col1'], $options->getConvertEmptyValuesToNull());
        self::assertTrue($options->isIncremental());
        self::assertTrue($options->useTimestamp());
        self::assertEquals(1, $options->getNumberOfIgnoredLines());
        self::assertEquals('MANAGED_IDENTITY', $options->getImportCredentialsType());
    }
}
