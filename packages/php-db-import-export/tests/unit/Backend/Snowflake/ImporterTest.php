<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;

class ImporterTest extends TestCase
{
    use MockConnectionTrait;
    use ABSSourceTrait;

    public function testGetAdapterMoreThanOneSupports(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Snowflake\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([Storage\ABS\SnowflakeImportAdapter::class, Storage\ABS\SnowflakeImportAdapter::class]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'More than one suitable adapter found for Snowflake importer with source: "Keboola\Db\ImportExport\Storage\ABS\SourceFile", destination "Keboola\Db\ImportExport\Storage\Snowflake\Table".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new ImportOptions());
    }

    public function testGetAdapterNoAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Snowflake\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([
            Storage\Snowflake\SnowflakeImportAdapter::class,
            Storage\Snowflake\SnowflakeImportAdapter::class,
        ]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'No suitable adapter found for Snowflake importer with source: "Keboola\Db\ImportExport\Storage\ABS\SourceFile", destination "Keboola\Db\ImportExport\Storage\Snowflake\Table".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new ImportOptions());
    }

    public function testGetAdapterInvalidAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Snowflake\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([Storage\Synapse\SynapseImportAdapter::class]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'Each snowflake import adapter must implement "Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new ImportOptions());
    }
}
