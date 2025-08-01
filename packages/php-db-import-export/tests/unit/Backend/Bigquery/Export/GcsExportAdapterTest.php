<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Bigquery\Export;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Keboola\Db\ImportExport\Backend\Bigquery\Export\GcsExportAdapter;
use Keboola\Db\ImportExport\ExportFileType;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use PHPUnit\Framework\MockObject\MockObject;
use SplFileInfo;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class GcsExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommand(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'bucket',
            'path',
        );

        /** @var Storage\GCS\DestinationFile&MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getRelativePath')->willReturn($path);

        /** @var QueryJobConfiguration&MockObject $query */
        $query = $this->createMock(QueryJobConfiguration::class);
        $query->expects(self::once())->method('parameters')->with([])->willReturnSelf();

        /** @var BigQueryClient&MockObject $bqClient */
        $bqClient = $this->getMockBuilder(BigQueryClient::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['runQuery', 'query'])
            ->getMock();
        $bqClient->expects(self::once())->method('runQuery')->with($query);
        $bqClient->expects(self::once())->method('query')
            ->with(
                <<<EOT
EXPORT DATA
OPTIONS (
    uri = 'gs://bucket/path*.csv'
    ,format = 'CSV'
    ,overwrite = true
    ,header = false
    ,field_delimiter = ','
    
) AS (
    SELECT * FROM `schema`.`table`
);
EOT,
            )->willReturn($query);

        $adapter = new GcsExportAdapter($bqClient);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table');
        $options = new ExportOptions(true, ExportOptions::MANIFEST_SKIP);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'bucket',
            'path',
        );

        /** @var Storage\GCS\DestinationFile&MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getRelativePath')->willReturn($path);

        /** @var QueryJobConfiguration&MockObject $query */
        $query = $this->createMock(QueryJobConfiguration::class);
        $query->expects(self::once())->method('parameters')->with([])->willReturnSelf();

        /** @var BigQueryClient&MockObject $bqClient */
        $bqClient = $this->getMockBuilder(BigQueryClient::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['runQuery', 'query'])
            ->getMock();
        $bqClient->expects(self::once())->method('runQuery')->with($query);
        $bqClient->expects(self::once())->method('query')
            ->with(
                <<<EOT
EXPORT DATA
OPTIONS (
    uri = 'gs://bucket/path*.csv.gz'
    ,format = 'CSV'
    ,overwrite = true
    ,header = false
    ,field_delimiter = ','
    ,compression='GZIP'
) AS (
    SELECT * FROM `schema`.`table`
);
EOT,
            )->willReturn($query);

        $adapter = new GcsExportAdapter($bqClient);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandQuery(): void
    {
        $source = new Storage\Bigquery\SelectSource('SELECT * FROM `schema`.`table`', ['param1' => 'value1']);
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'bucket',
            'path',
        );

        /** @var Storage\GCS\DestinationFile&MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getRelativePath')->willReturn($path);

        /** @var QueryJobConfiguration&MockObject $query */
        $query = $this->createMock(QueryJobConfiguration::class);
        $query->expects(self::once())->method('parameters')->with(['param1' => 'value1'])->willReturnSelf();

        /** @var BigQueryClient&MockObject $bqClient */
        $bqClient = $this->getMockBuilder(BigQueryClient::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['runQuery', 'query'])
            ->getMock();
        $bqClient->expects(self::once())->method('runQuery')->with($query);
        $bqClient->expects(self::once())->method('query')
            ->with(
                <<<EOT
EXPORT DATA
OPTIONS (
    uri = 'gs://bucket/path*.csv'
    ,format = 'CSV'
    ,overwrite = true
    ,header = false
    ,field_delimiter = ','
    
) AS (
    SELECT * FROM `schema`.`table`
);
EOT,
            )->willReturn($query);

        $adapter = new GcsExportAdapter($bqClient);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandWithParquet(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP, [], ExportFileType::PARQUET);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'bucket',
            'path',
        );

        /** @var Storage\GCS\DestinationFile&MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getRelativePath')->willReturn($path);

        /** @var QueryJobConfiguration&MockObject $query */
        $query = $this->createMock(QueryJobConfiguration::class);
        $query->expects(self::once())->method('parameters')->with([])->willReturnSelf();

        /** @var BigQueryClient&MockObject $bqClient */
        $bqClient = $this->getMockBuilder(BigQueryClient::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['runQuery', 'query'])
            ->getMock();
        $bqClient->expects(self::once())->method('runQuery')->with($query);
        $bqClient->expects(self::once())->method('query')
            ->with(
                <<<EOT
EXPORT DATA
OPTIONS (
    uri = 'gs://bucket/path*.parquet'
    ,format = 'PARQUET'
    ,overwrite = true
    
) AS (
    SELECT * FROM `schema`.`table`
);
EOT,
            )->willReturn($query);

        $adapter = new GcsExportAdapter($bqClient);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandWithCompressedParquet(): void
    {
        $source = new Storage\Bigquery\Table('schema', 'table');
        $options = new ExportOptions(true, ExportOptions::MANIFEST_SKIP, [], ExportFileType::PARQUET);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'bucket',
            'path',
        );

        /** @var Storage\GCS\DestinationFile&MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getRelativePath')->willReturn($path);

        /** @var QueryJobConfiguration&MockObject $query */
        $query = $this->createMock(QueryJobConfiguration::class);
        $query->expects(self::once())->method('parameters')->with([])->willReturnSelf();

        /** @var BigQueryClient&MockObject $bqClient */
        $bqClient = $this->getMockBuilder(BigQueryClient::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['runQuery', 'query'])
            ->getMock();
        $bqClient->expects(self::once())->method('runQuery')->with($query);
        $bqClient->expects(self::once())->method('query')
            ->with(
                <<<EOT
EXPORT DATA
OPTIONS (
    uri = 'gs://bucket/path*.parquet'
    ,format = 'PARQUET'
    ,overwrite = true
    ,compression='SNAPPY'
) AS (
    SELECT * FROM `schema`.`table`
);
EOT,
            )->willReturn($query);

        $adapter = new GcsExportAdapter($bqClient);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }
}
