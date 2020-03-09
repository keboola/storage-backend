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
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SynapseImportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');

        /** @var Connection|MockObject $source */
        $conn = $this->createMock(Connection::class);
        $conn->expects($this->once())->method('getDatabasePlatform')->willReturn(
            new SQLServer2012Platform()
        );
        $conn->expects($this->any())->method('quote')->willReturnCallback(static function ($input) {
            return QuoteHelper::quote($input);
        });

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SynapseImportAdapter($source);
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable',
            $conn
        );

        $this->assertSame([
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
            ,
        ], $commands);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['https://url']);
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');

        /** @var Connection|MockObject $source */
        $conn = $this->createMock(Connection::class);
        $conn->expects($this->once())->method('getDatabasePlatform')->willReturn(
            new SQLServer2012Platform()
        );
        $conn->expects($this->any())->method('quote')->willReturnCallback(static function ($input) {
            return QuoteHelper::quote($input);
        });

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions([], [], false, false, 1);
        $adapter = new SynapseImportAdapter($source);
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable',
            $conn
        );

        $this->assertSame([
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
            ,
        ], $commands);
    }
}
