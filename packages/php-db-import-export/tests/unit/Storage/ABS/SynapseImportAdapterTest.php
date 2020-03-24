<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\ABS\SynapseImportAdapter;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SynapseImportAdapterTest extends BaseTestCase
{
    use ABSSourceTrait;
    use MockConnectionTrait;

    public function testIsSupported(): void
    {
        $absSource = $this->createDummyABSSourceInstance('');
        $snowflakeTable = new Storage\Snowflake\Table('', '');
        $snowflakeSelectSource = new Storage\Snowflake\SelectSource('', []);
        $synapseTable = new Storage\Synapse\Table('', '');

        $this->assertTrue(
            SynapseImportAdapter::isSupported(
                $absSource,
                $synapseTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $snowflakeSelectSource,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $absSource,
                $snowflakeTable
            )
        );
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('exec')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sasToken'),
    FIELDQUOTE='\"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    ROWTERMINATOR='0x0A',
    IDENTITY_INSERT = 'OFF'
    
)
EOT
        );

        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SynapseImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
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
        $conn->expects($this->once())->method('exec')->with(
            <<<EOT
COPY INTO [schema].[stagingTable]
FROM 'https://url'
WITH (
    FILE_TYPE='CSV',
    CREDENTIAL=(IDENTITY='Shared Access Signature', SECRET='?sasToken'),
    FIELDQUOTE='\"',
    FIELDTERMINATOR=',',
    ENCODING = 'UTF8',
    ROWTERMINATOR='0x0A',
    IDENTITY_INSERT = 'OFF'
    ,FIRSTROW=2
)
EOT
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions([], [], false, false, 1);
        $adapter = new SynapseImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );

        $this->assertEquals(10, $count);
    }
}
