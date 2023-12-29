<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\TableDistribution;
use PHPUnit\Framework\TestCase;

class TableDistributionTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new TableDistribution();
        self::assertEquals([], $options->getDistributionColumnsNames());
        self::assertEquals('ROUND_ROBIN', $options->getDistributionName());
        self::assertFalse($options->isHashDistribution());
    }

    public function testDistributionValues(): void
    {
        $options = new TableDistribution(
            TableDistribution::TABLE_DISTRIBUTION_HASH,
            ['pk1'],
        );
        self::assertEquals(['pk1'], $options->getDistributionColumnsNames());
        self::assertEquals('HASH', $options->getDistributionName());
        self::assertTrue($options->isHashDistribution());
    }
}
