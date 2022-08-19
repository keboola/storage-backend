<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS\ManifestGenerator;

use Keboola\Db\ImportExport\Storage\ABS\ManifestGenerator\AbsSlicedManifestFromUnloadQueryResultGenerator;
use Keboola\Db\ImportExport\Storage\SlicedManifestGeneratorInterface;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Path\RelativePath;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class AbsSlicedManifestFromUnloadQueryResultGeneratorTest extends TestCase
{
    public function testGenerateAndSaveManifest(): void
    {
        /** @var MockObject|BlobRestProxy $mock */
        $mock = $this->getMockBuilder(BlobRestProxy::class)
            ->disableOriginalConstructor()
            ->setMethods(['getAccountName', 'createBlockBlob'])
            ->getMock();

        $mock->expects($this->exactly(2))->method('getAccountName')
            ->willReturn('keboola')
        ;

        $mock->expects($this->once())->method('createBlockBlob')
            ->with(
                'container',
                'prefix/xxxmanifest',
                //phpcs:ignore
                '{"entries":[{"url":"azure:\/\/keboola.blob.core.windows.net\/container\/17982.csv.gz_0_0_0.csv.gz","mandatory":true},{"url":"azure:\/\/keboola.blob.core.windows.net\/container\/17982.csv.gz_0_0_1.csv.gz","mandatory":true}]}'
            );

        $path = RelativePath::createFromRootAndPath(new AbsProvider(), 'container', 'prefix/xxx');

        $generator = new AbsSlicedManifestFromUnloadQueryResultGenerator($mock);
        $generator->generateAndSaveManifest($path, [
            ['FILE_NAME' => '17982.csv.gz_0_0_0.csv.gz', 'FILE_SIZE' => '10', 'ROW_COUNT' => '5'],
            ['FILE_NAME' => '17982.csv.gz_0_0_1.csv.gz', 'FILE_SIZE' => '25', 'ROW_COUNT' => '15'],
        ]);

        $this->assertInstanceOf(SlicedManifestGeneratorInterface::class, $generator);
    }
}
