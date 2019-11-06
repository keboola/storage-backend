<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Storage;

trait ABSSourceTrait
{
    protected function createDummyABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return new Storage\ABS\SourceFile(
            'absContainer',
            'azureCredentials',
            'absAccount',
            new CsvFile($file),
            $isSliced
        );
    }

    protected function createABSSourceDestinationInstance(
        string $filePath
    ): Storage\ABS\DestinationFile {
        return new Storage\ABS\DestinationFile(
            (string) getenv('ABS_CONTAINER_NAME'),
            $filePath,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME')
        );
    }

    protected function createABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return $this->createABSSourceInstanceFromCsv(new CsvFile($file), $isSliced);
    }

    protected function createABSSourceInstanceFromCsv(
        CsvFile $file,
        bool $isSliced = false
    ): Storage\ABS\SourceFile {
        return new Storage\ABS\SourceFile(
            (string) getenv('ABS_CONTAINER_NAME'),
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
