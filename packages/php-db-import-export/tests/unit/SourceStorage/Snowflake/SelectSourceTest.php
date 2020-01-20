<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use PHPUnit\Framework\TestCase;

class SelectSourceTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Snowflake\SelectSource('SELECT * FROM "SCHEMA"."TABLE"', ['prop' => 1]);

        $this->assertEquals('(SELECT * FROM "SCHEMA"."TABLE")', $source->getFromStatement());
        $this->assertEquals('SELECT * FROM "SCHEMA"."TABLE"', $source->getQuery());
        $this->assertSame(['prop' => 1], $source->getQueryBindings());

        $this->expectException(NoBackendAdapterException::class);
        $source->getBackendImportAdapter((new class implements ImporterInterface {
            public function importTable(
                Storage\SourceInterface $source,
                Storage\DestinationInterface $destination,
                ImportOptions $options
            ): Result {
                // only a stub
            }
        }));
    }
}
