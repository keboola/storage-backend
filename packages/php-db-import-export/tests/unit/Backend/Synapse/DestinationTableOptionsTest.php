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
            ['pk1', 'pk1'],
            DestinationTableOptions::PRIMARY_KEYS_DEFINITION_METADATA
        );
        self::assertEquals(['pk1', 'pk1', 'col1', 'col2'], $options->getColumnNamesInOrder());
        self::assertEquals(['pk1', 'pk1'], $options->getPrimaryKeys());
        self::assertTrue($options->isPrimaryKeyFromMetadata());
    }
}
