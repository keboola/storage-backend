<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3\ManifestGenerator;

use Aws\S3\S3Client;
use Keboola\Db\ImportExport\Storage\S3\ManifestGenerator\S3SlicedManifestFromUnloadQueryResultGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class S3SlicedManifestFromUnloadQueryResultGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        /** @var MockObject|S3Client $s3ClientMock */
        $s3ClientMock = $this->getMockBuilder(S3Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['putObject'])
            ->getMock();
        $s3ClientMock->expects($this->once())->method('putObject')
            ->with([
                'Bucket' => 'tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q',
                //phpcs:ignore
                'Key' => 'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gzmanifest',
                //phpcs:ignore
                'Body' => '{"entries":[{"url":"s3:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_0.csv.gz","mandatory":true},{"url":"s3:\/\/tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q\/permanent\/256\/snapshots\/in\/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985\/languages\/17982.csv.gz_0_0_1.csv.gz","mandatory":true}]}',
                'ServerSideEncryption' => 'AES256',
            ]);

        $path = RelativePath::createFromRootAndPath(
            new S3Provider(),
            'tomasfejfar-kbc-services-filestorag-s3filesbucket-ggrrgg35547q',
            'permanent/256/snapshots/in/c-API-tests-e46793dac57ccf8cefb82ae9b8c05844cfabf985/languages/17982.csv.gz'
        );

        $generator = new S3SlicedManifestFromUnloadQueryResultGenerator($s3ClientMock);
        $generator->generateAndSaveManifest($path, [
            ['FILE_NAME' => '17982.csv.gz_0_0_0.csv.gz', 'FILE_SIZE' => '10', 'ROW_COUNT' => '5'],
            ['FILE_NAME' => '17982.csv.gz_0_0_1.csv.gz', 'FILE_SIZE' => '25', 'ROW_COUNT' => '15'],
        ]);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
