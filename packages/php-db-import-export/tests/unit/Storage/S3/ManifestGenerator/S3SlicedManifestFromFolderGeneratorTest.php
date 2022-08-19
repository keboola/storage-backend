<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3\ManifestGenerator;

use Aws\S3\S3Client;
use Keboola\Db\ImportExport\Storage\S3\ManifestGenerator\S3SlicedManifestFromFolderGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\S3\S3Provider;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class S3SlicedManifestFromFolderGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        /** @var MockObject|S3Client $mock */
        $mock = $this->getMockBuilder(S3Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['getIterator', 'putObject'])
            ->getMock();
        $mock->expects($this->once())->method('getIterator')->willReturn([
            [
                'Key' => 'key1',
            ],
            [
                'Key' => 'key2',
            ],
        ]);
        $mock->expects($this->once())->method('putObject')
            ->with([
                'Bucket' => 'bucket',
                'Key' => 'prefix/xxxmanifest',
                //phpcs:ignore
                'Body' => '{"entries":[{"url":"s3:\/\/bucket\/key1","mandatory":true},{"url":"s3:\/\/bucket\/key2","mandatory":true}]}',
                'ServerSideEncryption' => 'AES256',
            ]);

        $path = RelativePath::createFromRootAndPath(new S3Provider(), 'bucket', 'prefix/xxx');

        $generator = new S3SlicedManifestFromFolderGenerator($mock);
        $generator->generateAndSaveManifest($path);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
