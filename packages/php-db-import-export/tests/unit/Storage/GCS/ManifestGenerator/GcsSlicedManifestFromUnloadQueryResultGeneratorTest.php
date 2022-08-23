<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS\ManifestGenerator;

use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator\GcsSlicedManifestFromUnloadQueryResultGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class GcsSlicedManifestFromUnloadQueryResultGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        $bucketMock = $this->getMockBuilder(Bucket::class)
            ->disableOriginalConstructor()
            ->setMethods(['upload'])
            ->getMock();
        $bucketMock->expects($this->once())->method('upload')
            ->with(
                //phpcs:ignore
                '{"entries":[{"url":"gs:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_0.csv.gz","mandatory":true},{"url":"gs:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_1.csv.gz","mandatory":true}]}',
                //phpcs:ignore
                ['name' => 'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gzmanifest']
            );

        /** @var MockObject|StorageClient $gcsClientMock */
        $gcsClientMock = $this->getMockBuilder(StorageClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['bucket'])
            ->getMock();
        $gcsClientMock->expects($this->once())->method('bucket')->willReturn($bucketMock);

        $path = RelativePath::createFromRootAndPath(
            new GcsProvider(),
            'tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q',
            'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gz'
        );

        $generator = new GcsSlicedManifestFromUnloadQueryResultGenerator($gcsClientMock);
        $generator->generateAndSaveManifest($path, [
            ['FILE_NAME' => '17982.csv.gz_0_0_0.csv.gz', 'FILE_SIZE' => '10', 'ROW_COUNT' => '5'],
            ['FILE_NAME' => '17982.csv.gz_0_0_1.csv.gz', 'FILE_SIZE' => '25', 'ROW_COUNT' => '15'],
        ]);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
