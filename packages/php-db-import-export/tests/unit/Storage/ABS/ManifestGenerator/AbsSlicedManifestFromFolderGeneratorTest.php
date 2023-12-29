<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS\ManifestGenerator;

use Keboola\Db\ImportExport\Storage\ABS\ManifestGenerator\AbsSlicedManifestFromFolderGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Path\RelativePath;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsResult;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class AbsSlicedManifestFromFolderGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        /** @var MockObject|BlobRestProxy $mock */
        $mock = $this->getMockBuilder(BlobRestProxy::class)
            ->disableOriginalConstructor()
            ->setMethods(['listBlobs', 'createBlockBlob'])
            ->getMock();

        /** @var MockObject|ListBlobsResult $blobResultMock */
        $blobResultMock = $this->getMockBuilder(ListBlobsResult::class)
            ->disableOriginalConstructor()
            ->setMethods(['getBlobs', 'getNextMarker'])
            ->getMock();
        $blob1 = new Blob();
        $blob1->setUrl('azure://1');
        $blob2 = new Blob();
        $blob2->setUrl('azure://2');
        $blobResultMock->method('getBlobs')->willReturn([
            $blob1,
            $blob2,
        ]);
        $blobResultMock->method('getNextMarker')->willReturn(null);

        $mock->method('listBlobs')->willReturn($blobResultMock);
        $mock->method('createBlockBlob')
            ->with(
                'container',
                'prefix/xxxmanifest',
                '{"entries":[{"url":"azure:\/\/1","mandatory":true},{"url":"azure:\/\/2","mandatory":true}]}',
            );

        $path = RelativePath::createFromRootAndPath(new AbsProvider(), 'container', 'prefix/xxx');

        $generator = new AbsSlicedManifestFromFolderGenerator($mock);
        $generator->generateAndSaveManifest($path);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
