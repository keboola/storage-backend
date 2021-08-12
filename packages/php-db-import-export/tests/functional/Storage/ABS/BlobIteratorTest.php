<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Storage\ABS;

use Keboola\Db\ImportExport\Storage\ABS\BlobIterator;
use Keboola\FileStorage\Abs\ClientFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class BlobIteratorTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testIterator(): void
    {
        $this->getClient();

        $options = new ListBlobsOptions();
        $options->setPrefix('sliced/2cols-large'); // 1500 slices + 4 other files
        $iterator = new BlobIterator(
            $this->getClient(),
            (string) getenv('ABS_CONTAINER_NAME'),
            $options
        );

        $count = 0;
        foreach ($iterator as $blob) {
            $this->assertStringStartsWith('sliced/2cols-large', $blob->getName());
            $count++;
        }

        $this->assertGreaterThan(1500, $count);
        $this->assertLessThan(1510, $count);
    }

    public function testIteratorLimit100(): void
    {
        $this->getClient();

        $options = new ListBlobsOptions();
        $options->setPrefix('sliced/2cols-large'); // 1500 slices + 4 other files
        $options->setMaxResults(100);
        $iterator = new BlobIterator(
            $this->getClient(),
            (string) getenv('ABS_CONTAINER_NAME'),
            $options
        );

        $count = 0;
        foreach ($iterator as $blob) {
            $this->assertStringStartsWith('sliced/2cols-large', $blob->getName());
            $count++;
        }

        $this->assertGreaterThan(1500, $count);
        $this->assertLessThan(1510, $count);
    }

    private function getClient(): BlobRestProxy
    {
        $SASConnectionString = sprintf(
            '%s=https://%s.%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            (string) getenv('ABS_ACCOUNT_NAME'),
            Resources::BLOB_BASE_DNS_NAME,
            Resources::SAS_TOKEN_NAME,
            $this->getCredentialsForAzureContainer(
                (string) getenv('ABS_CONTAINER_NAME'),
                'rl'
            )
        );

        return ClientFactory::createClientFromConnectionString(
            $SASConnectionString
        );
    }
}
