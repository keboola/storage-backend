<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\DestinationTableOptions;
use PHPUnit\Framework\TestCase;

class DestinationTableOptionsTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $options = new DestinationTableOptions(
            ['pk1', 'pk1', 'col1', 'col2'],
            ['pk1', 'pk1']
        );
        self::assertEquals(['pk1', 'pk1', 'col1', 'col2'], $options->getColumnNamesInOrder());
        self::assertEquals(['pk1', 'pk1'], $options->getPrimaryKeys());
        self::assertEquals('ROUND_ROBIN', $options->getDistribution());
        self::assertEquals([], $options->getDistributionColumnsNames());
    }

    public function testDistributionValues(): void
    {
        $options = new DestinationTableOptions(
            ['pk1', 'pk1', 'col1', 'col2'],
            ['pk1', 'pk1'],
            DestinationTableOptions::TABLE_DISTRIBUTION_HASH,
            ['pk1']
        );
        self::assertEquals(['pk1', 'pk1', 'col1', 'col2'], $options->getColumnNamesInOrder());
        self::assertEquals(['pk1', 'pk1'], $options->getPrimaryKeys());
        self::assertEquals('HASH', $options->getDistribution());
        self::assertEquals(['pk1'], $options->getDistributionColumnsNames());
    }
}
