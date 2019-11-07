<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Db\ImportExport\ExportOptions;
use PHPUnit\Framework\MockObject\MockObject;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommand(): void
    {
        /** @var Storage\ABS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\ABS\DestinationFile::class);
        $destination->expects(self::once())->method('getContainerUrl')->willReturn('containerUrl');
        $destination->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions();
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $commands = $adapter->getCopyCommand(
            $source,
            $options
        );

        self::assertSame(
            <<<EOT
COPY INTO 'containerUrl' 
FROM "schema"."table"
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
EOT
            ,
            $commands
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        /** @var Storage\ABS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\ABS\DestinationFile::class);
        $destination->expects(self::once())->method('getContainerUrl')->willReturn('containerUrl');
        $destination->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(null, true);
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $commands = $adapter->getCopyCommand(
            $source,
            $options
        );

        self::assertSame(
            <<<EOT
COPY INTO 'containerUrl' 
FROM "schema"."table"
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='GZIP'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
EOT
            ,
            $commands
        );
    }


    public function testGetCopyCommandQuery(): void
    {
        /** @var Storage\ABS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\ABS\DestinationFile::class);
        $destination->expects(self::once())->method('getContainerUrl')->willReturn('containerUrl');
        $destination->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions('SELECT * FROM "schema"."table"');
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $commands = $adapter->getCopyCommand(
            $source,
            $options
        );

        self::assertSame(
            <<<EOT
COPY INTO 'containerUrl' 
FROM (SELECT * FROM "schema"."table")
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
EOT
            ,
            $commands
        );
    }
}
