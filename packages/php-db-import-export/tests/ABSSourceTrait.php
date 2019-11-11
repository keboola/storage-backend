<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\SourceStorage;

trait ABSSourceTrait
{
    protected function createDummyABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            'absContainer',
            $file,
            'azureCredentials',
            'absAccount',
            new CsvFile($file),
            $isSliced
        );
    }

    protected function createABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            (string) getenv('ABS_CONTAINER_NAME'),
            $file,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            new CsvFile($file), //TODO: create file inside or use only CSV file
            $isSliced
        );
    }

    protected function createABSSourceInstanceFromCsv(
        CsvFile $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            (string) getenv('ABS_CONTAINER_NAME'),
            $file->getFilename(),
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            $file,
            $isSliced
        );
    }

    protected function getCredentialsForAzureContainer(
        string $container
    ): string {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            'rwl',
            $expirationDate,
            (new DateTime())
        );
    }
}
