<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\BaseFile;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class BaseFileTest extends BaseTestCase
{
    public function testDefaultValues(): void
    {
        $baseFile = new class(
            'absContainer',
            'file.csv',
            'azureCredentials',
            'absAccount'
        ) extends Storage\ABS\BaseFile {
        };
        self::assertEquals('file.csv', $baseFile->getFilePath());
        self::assertEquals(
            'azure://absAccount.blob.core.windows.net/absContainer/',
            $baseFile->getContainerUrl($baseFile::PROTOCOL_AZURE),
        );
        self::assertEquals(
            'https://absAccount.blob.core.windows.net/absContainer/',
            $baseFile->getContainerUrl($baseFile::PROTOCOL_HTTPS),
        );
        self::assertEquals('azureCredentials', $baseFile->getSasToken());
    }
}
