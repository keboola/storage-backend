<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Generator;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
        self::assertInstanceOf(Storage\ABS\BaseFile::class, $source);
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('file.csv', $source->getFilePath());
        self::assertEquals([], $source->getColumnsNames());
        self::assertNull($source->getPrimaryKeysNames());
        self::assertSame('azureCredentials', $source->getSasToken());
    }

    /**
     * @dataProvider getFilePartsProvider
     */
    public function testGetFilepathParts(string $file, string $expPrefix, string $expFile): void
    {
        $source = $this->createDummyABSSourceInstance($file);
        self::assertEquals($expPrefix, $source->getPrefix());
        self::assertEquals($expFile, $source->getFileName());
    }

    public function getFilePartsProvider(): Generator
    {
        yield [
            'data/shared/file.csv',
            'data/shared/',
            'file.csv',
        ];
        yield [
            'file.csv',
            '',
            'file.csv',
        ];
        yield [
            'azure://absAccount.blob.core.windows.net/absContainer/data/shared/file.csv',
            'data/shared/',
            'file.csv',
        ];
    }
}
