<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS\ManifestGenerator;

use ArrayIterator;
use Google\Cloud\Core\Upload\StreamableUploader;
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\ObjectIterator;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Keboola\Db\ImportExport\Storage\GCS\ManifestGenerator\GcsSlicedManifestFromFolderGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class GCSSlicedManifestFromFolderGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        $streamableUploaderMock = $this->getMockBuilder(StreamableUploader::class)
            ->disableOriginalConstructor()
            ->getMock();
        $streamableUploaderMock->method('getResumeUri')->willReturn('');
        $streamableUploaderMock->method('upload')->willReturn([]);

        $object1 = $this->createMock(StorageObject::class);
        $object1->expects($this->once())
            ->method('name')
            ->willReturn('prefix/xxx/obj1_000000.csv');
        $object2 = $this->createMock(StorageObject::class);
        $object2->expects($this->once())
            ->method('name')
            ->willReturn('prefix/xxx/obj2_000000.csv');

        $bucket = $this->createMock(Bucket::class);
        $bucket->expects($this->once())
            ->method('objects')
            ->with(['prefix' => 'prefix/xxx'])
            ->willReturn($this->createObjectIterator([
                $object1,
                $object2,
            ]));
        $bucket->expects($this->exactly(2))
            ->method('name')
            ->willReturn('bucket1');

        $bucket->expects($this->once())->method('getStreamableUploader')
            ->willReturn($streamableUploaderMock);

        $clientMock = $this->createMock(StorageClient::class);
        $clientMock
            ->expects($this->once())
            ->method('bucket')
            ->with('bucket1')
            ->willReturn($bucket);

        $path = RelativePath::createFromRootAndPath(new GcsProvider(), 'bucket1', 'prefix/xxx');

        $generator = new GcsSlicedManifestFromFolderGenerator($clientMock);
        $generator->generateAndSaveManifest($path);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }

    /**
     * @param StorageObject[] $items
     * @return MockObject|ObjectIterator
     */
    private function createObjectIterator(array $items = []): ObjectIterator
    {
        $someIterator = $this->createMock(ObjectIterator::class);

        $iterator = new ArrayIterator($items);

        $someIterator
            ->method('rewind')
            ->willReturnCallback(function () use ($iterator): void {
                $iterator->rewind();
            });

        $someIterator
            ->method('current')
            ->willReturnCallback(function () use ($iterator) {
                return $iterator->current();
            });

        $someIterator
            ->method('key')
            ->willReturnCallback(function () use ($iterator) {
                return $iterator->key();
            });

        $someIterator
            ->method('next')
            ->willReturnCallback(function () use ($iterator): void {
                $iterator->next();
            });

        $someIterator
            ->method('valid')
            ->willReturnCallback(function () use ($iterator): bool {
                return $iterator->valid();
            });

        return $someIterator;
    }
}
