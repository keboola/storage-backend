<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS;

use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\GCSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class DestinationFileTest extends BaseTestCase
{
    use GCSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = new Storage\GCS\DestinationFile(
            'bucket',
            'file.csv',
            'integration',
            [
                'type' => '',
                'project_id' => '',
                'private_key_id' => '',
                'private_key' => '',
                'client_email' => '',
                'client_id' => '',
                'auth_uri' => '',
                'token_uri' => '',
                'auth_provider_x509_cert_url' => '',
                'client_x509_cert_url' => '',
            ]
        );
        self::assertInstanceOf(Storage\GCS\BaseFile::class, $source);
        self::assertInstanceOf(Storage\DestinationInterface::class, $source);
        self::assertEquals('file.csv', $source->getFilePath());
        self::assertEquals('integration', $source->getStorageIntegrationName());
        self::assertEquals('bucket', $source->getBucket());
        self::assertEquals('gcs://bucket', $source->getGcsPrefix());
    }
}
