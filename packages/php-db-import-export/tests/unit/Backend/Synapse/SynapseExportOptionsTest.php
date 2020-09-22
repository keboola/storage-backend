<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;
use PHPUnit\Framework\TestCase;

class SynapseExportOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new SynapseExportOptions();

        self::assertFalse($options->isCompressed());
        self::assertSame('MASTER_KEY', $options->getExportCredentialsType());
    }

    public function testValues(): void
    {
        $options = new SynapseExportOptions(true, SynapseExportOptions::CREDENTIALS_MANAGED_IDENTITY);

        self::assertTrue($options->isCompressed());
        self::assertSame('MANAGED_IDENTITY', $options->getExportCredentialsType());
    }
}
