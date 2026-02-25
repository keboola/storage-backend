<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Exporter;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\Exporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\S3ExportAdapter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ExporterTest extends TestCase
{
    public function testExportWithTimezone(): void
    {
        $expectedCopyResult = [
            ['FILE_NAME' => 'file', 'FILE_SIZE' => '0', 'ROW_COUNT' => '10'],
        ];

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);

        $conn->expects(self::exactly(2))
            ->method('executeStatement')
            ->willReturnCallback(function (string $sql): int {
                static $callIndex = 0;
                $callIndex++;
                if ($callIndex === 1) {
                    self::assertSame("ALTER SESSION SET TIMEZONE = 'UTC'", $sql);
                } else {
                    self::assertSame('ALTER SESSION UNSET TIMEZONE', $sql);
                }
                return 0;
            });

        $conn->expects(self::once())->method('executeQuery');
        $conn->expects(self::once())
            ->method('fetchAllAssociative')
            ->willReturn($expectedCopyResult);

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->method('getFilePath')->willReturn('xxx/path');
        $destination->method('getKey')->willReturn('key');
        $destination->method('getSecret')->willReturn('secret');
        $destination->method('getRegion')->willReturn('region');
        $destination->method('getS3Prefix')->willReturn('s3://bucketUrl');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(timezone: 'UTC');

        $exporter = new Exporter($conn);
        $exporter->setAdapters([S3ExportAdapter::class]);
        $result = $exporter->exportTable($source, $destination, $options);

        self::assertSame($expectedCopyResult, $result);
    }

    public function testExportWithoutTimezone(): void
    {
        $expectedCopyResult = [
            ['FILE_NAME' => 'file', 'FILE_SIZE' => '0', 'ROW_COUNT' => '10'],
        ];

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);

        $conn->expects(self::never())->method('executeStatement');

        $conn->expects(self::once())->method('executeQuery');
        $conn->expects(self::once())
            ->method('fetchAllAssociative')
            ->willReturn($expectedCopyResult);

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->method('getFilePath')->willReturn('xxx/path');
        $destination->method('getKey')->willReturn('key');
        $destination->method('getSecret')->willReturn('secret');
        $destination->method('getRegion')->willReturn('region');
        $destination->method('getS3Prefix')->willReturn('s3://bucketUrl');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions();

        $exporter = new Exporter($conn);
        $exporter->setAdapters([S3ExportAdapter::class]);
        $result = $exporter->exportTable($source, $destination, $options);

        self::assertSame($expectedCopyResult, $result);
    }

    public function testTimezoneIsUnsetOnExportFailure(): void
    {
        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);

        $conn->expects(self::exactly(2))
            ->method('executeStatement')
            ->willReturnCallback(function (string $sql): int {
                static $callIndex = 0;
                $callIndex++;
                if ($callIndex === 1) {
                    self::assertSame("ALTER SESSION SET TIMEZONE = 'Europe/Prague'", $sql);
                } else {
                    self::assertSame('ALTER SESSION UNSET TIMEZONE', $sql);
                }
                return 0;
            });

        $conn->expects(self::once())
            ->method('executeQuery')
            ->willThrowException(new RuntimeException('Export failed'));

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->method('getFilePath')->willReturn('xxx/path');
        $destination->method('getKey')->willReturn('key');
        $destination->method('getSecret')->willReturn('secret');
        $destination->method('getRegion')->willReturn('region');
        $destination->method('getS3Prefix')->willReturn('s3://bucketUrl');

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(timezone: 'Europe/Prague');

        $exporter = new Exporter($conn);
        $exporter->setAdapters([S3ExportAdapter::class]);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Export failed');
        $exporter->exportTable($source, $destination, $options);
    }
}
