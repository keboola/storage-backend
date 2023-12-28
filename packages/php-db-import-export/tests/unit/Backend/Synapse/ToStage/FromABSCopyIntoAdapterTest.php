<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\FromABSCopyIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromABSCopyIntoAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');
        $source->expects($this->once())->method('getLineEnding')->willReturn('lf');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeStatement')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sasToken'),
    FIELDQUOTE='"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    ROWTERMINATOR='0x0A',
    IDENTITY_INSERT = 'OFF'
    
)
EOT,
        );

        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsWindowsLineEnding(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');
        $source->expects($this->once())->method('getLineEnding')->willReturn('crlf');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeStatement')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sasToken'),
    FIELDQUOTE='"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    
    IDENTITY_INSERT = 'OFF'
    
)
EOT,
        );

        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeStatement')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sasToken'),
    FIELDQUOTE='"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    
    IDENTITY_INSERT = 'OFF'
    ,FIRSTROW=2
)
EOT,
        );
        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions([], false, false, 1);
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsManagedIdentity(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeStatement')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Managed Identity'),
    FIELDQUOTE='"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    
    IDENTITY_INSERT = 'OFF'
    
)
EOT,
        );

        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions(
            [],
            false,
            false,
            SynapseImportOptions::SKIP_NO_LINE,
            SynapseImportOptions::CREDENTIALS_MANAGED_IDENTITY,
        );
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }
}
