<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\ABSSourceTrait;

class ImporterTest extends TestCase
{
    use MockConnectionTrait;
    use ABSSourceTrait;

    public function testGetAdapterMoreThanOneSupports(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Synapse\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([Storage\ABS\SynapseImportAdapter::class, Storage\ABS\SynapseImportAdapter::class]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'More than one suitable adapter found for Synapse importer with source: "Keboola\Db\ImportExport\Storage\ABS\SourceFile", destination "Keboola\Db\ImportExport\Storage\Synapse\Table".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new SynapseImportOptions());
    }

    public function testGetAdapterNoAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Synapse\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([
            Storage\Synapse\SynapseImportAdapter::class,
            Storage\Synapse\SynapseImportAdapter::class,
        ]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'No suitable adapter found for Synapse importer with source: "Keboola\Db\ImportExport\Storage\ABS\SourceFile", destination "Keboola\Db\ImportExport\Storage\Synapse\Table".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new SynapseImportOptions());
    }

    public function testGetAdapterInvalidAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('');
        $destination = new Storage\Synapse\Table('', '');

        $importer = new Importer($this->mockConnection());
        $importer->setAdapters([Storage\Snowflake\SnowflakeImportAdapter::class]);

        $this->expectExceptionMessage(
        // phpcs:ignore
            'Each Synapse import adapter must implement "Keboola\Db\ImportExport\Backend\Synapse\SynapseImportAdapterInterface".'
        );
        $this->expectException(\Throwable::class);
        $importer->importTable($source, $destination, new SynapseImportOptions());
    }
}
