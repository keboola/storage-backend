<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Db\Import\Snowflake\Connection;
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

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAll')->with(
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
MAX_FILE_SIZE=50000000
EOT
            ,
            []
        );

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions();
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $adapter->runCopyCommand(
            $source,
            $options,
            $conn
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        /** @var Storage\ABS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\ABS\DestinationFile::class);
        $destination->expects(self::once())->method('getContainerUrl')->willReturn('containerUrl');
        $destination->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAll')->with(
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
MAX_FILE_SIZE=50000000
EOT
            ,
            []
        );

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(true);
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $adapter->runCopyCommand(
            $source,
            $options,
            $conn
        );
    }

    public function testGetCopyCommandQuery(): void
    {
        /** @var Storage\ABS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\ABS\DestinationFile::class);
        $destination->expects(self::once())->method('getContainerUrl')->willReturn('containerUrl');
        $destination->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAll')->with(
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
MAX_FILE_SIZE=50000000
EOT
            ,
            []
        );

        $source = new Storage\Snowflake\SelectSource('SELECT * FROM "schema"."table"');
        $options = new ExportOptions();
        $adapter = new Storage\ABS\SnowflakeExportAdapter($destination);
        $adapter->runCopyCommand(
            $source,
            $options,
            $conn
        );
    }
}
