<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Keboola\Db\ImportExport\Backend\Synapse\DestinationTableOptions;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\ABS\DestinationFile;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use PHPUnit\Framework\TestCase;
use Keboola\Db\Import\Exception;

class AssertTest extends TestCase
{
    public function testAssertColumnsPass(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertColumns(
            new class implements SourceInterface {
                public function getColumnsNames(): array
                {
                    return ['name', 'id'];
                }

                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new DestinationTableOptions(
                ['id', 'name'],
                []
            )
        );
    }

    public function testAssertNoColumnsFail(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('No columns found in CSV file.');
        Assert::assertColumns(
            new class implements SourceInterface {
                public function getColumnsNames(): array
                {
                    return [];
                }

                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new DestinationTableOptions(
                ['id', 'name'],
                []
            )
        );
    }

    public function testAssertColumnsNotMatch(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: unexpected');
        Assert::assertColumns(
            new class implements SourceInterface {
                public function getColumnsNames(): array
                {
                    return ['name', 'id', 'unexpected'];
                }

                public function getPrimaryKeysNames(): ?array
                {
                    return null;
                }
            },
            new DestinationTableOptions(
                ['id', 'name'],
                []
            )
        );
    }

    public function testAssertIsSynapseTableDestinationPass(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertIsSynapseTableDestination(new Table('', ''));
    }

    public function testAssertIsSynapseTableDestinationNoTable(): void
    {
        $this->expectException(\Throwable::class);
        $this->expectExceptionMessage(
        // phpcs:ignore
            'Only "Keboola\Db\ImportExport\Storage\Synapse\Table" is supported as destination "Keboola\Db\ImportExport\Storage\ABS\DestinationFile" provided.'
        );
        Assert::assertIsSynapseTableDestination(new DestinationFile('', '', '', ''));
    }

    public function testAssertSynapseImportOptions(): void
    {
        $this->expectNotToPerformAssertions();
        Assert::assertSynapseImportOptions(new SynapseImportOptions());
    }

    public function testAssertValidSource(): void
    {
        $this->expectException(\Throwable::class);
        $this->expectExceptionMessage(
        // phpcs:ignore
            'Synapse importer expect $options to be instance of "Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions", "Keboola\Db\ImportExport\ImportOptions" given.'
        );
        Assert::assertSynapseImportOptions(new ImportOptions());
    }
}
