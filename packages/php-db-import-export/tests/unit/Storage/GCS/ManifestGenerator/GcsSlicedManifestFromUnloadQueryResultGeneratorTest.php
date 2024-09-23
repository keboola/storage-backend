<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS\ManifestGenerator;

use Google\Cloud\Core\Upload\StreamableUploader;
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\WriteStream;
use Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator\GcsSlicedManifestFromUnloadQueryResultGenerator;
use Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator\WriteStreamFactory;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use PHPUnit\Framework\TestCase;

class GcsSlicedManifestFromUnloadQueryResultGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        $writeStreamMock = $this->createMock(WriteStream::class);
        $writeStreamFactory = $this->createMock(WriteStreamFactory::class);
        $writeStreamFactory->method('createWriteStream')->willReturn($writeStreamMock);
        $streamableUploaderMock = $this->getMockBuilder(StreamableUploader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bucketMock = $this->createMock(Bucket::class);
        $bucketMock->method('getStreamableUploader')->willReturnCallback(
            function (WriteStream $w, array $o) use ($streamableUploaderMock) {
                //phpcs:ignore
                self::assertSame(['name' => 'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gzmanifest'], $o);
                return $streamableUploaderMock;
            },
        );

        $gcsClientMock = $this->createMock(StorageClient::class);
        $gcsClientMock->expects($this->once())->method('bucket')->willReturn($bucketMock);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q',
            'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gz',
        );

        $writtenData = '';
        $writeStreamMock->method('write')->willReturnCallback(function (string $data) use (&$writtenData) {
            $writtenData .= $data;
            return 0;
        });

        $generator = new GcsSlicedManifestFromUnloadQueryResultGenerator($gcsClientMock, $writeStreamFactory);
        $generator->generateAndSaveManifest($path, [
            ['FILE_NAME' => '17982.csv.gz_0_0_0.csv.gz', 'FILE_SIZE' => '10', 'ROW_COUNT' => '5'],
            ['FILE_NAME' => '17982.csv.gz_0_0_1.csv.gz', 'FILE_SIZE' => '25', 'ROW_COUNT' => '15'],
        ]);

        $this->assertSame(
        //phpcs:ignore
            '{"entries":[{"url":"gs:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_0.csv.gz","mandatory":true},{"url":"gs:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_1.csv.gz","mandatory":true}]}',
            $writtenData,
        );

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
