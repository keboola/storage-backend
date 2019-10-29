<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\File;

use Keboola\Csv\CsvFile;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class Azure
{
    public const IS_SLICED = true;
    public const IS_NOT_SLICED = false;

    /** @var string */
    private $container;

    /** @var string */
    private $filePath;

    /** @var string */
    private $sasToken;

    /** @var string */
    private $accountName;

    /** @var CsvFile */
    private $csvFile;

    /** @var boolean */
    private $isSliced;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvFile $csvFile,
        bool $isSliced
    ) {
        $this->container = $container;
        $this->filePath = $filePath;
        $this->sasToken = $sasToken;
        $this->accountName = $accountName;
        $this->csvFile = $csvFile;
        $this->isSliced = $isSliced;
    }

    public function getContainer(): string
    {
        return $this->container;
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }

    public function getSasToken(): string
    {
        return $this->sasToken;
    }

    public function getAccountName(): string
    {
        return $this->accountName;
    }

    public function getCsvFile(): CsvFile
    {
        return $this->csvFile;
    }

    public function getContainerUrl(): string
    {
        return sprintf(
            'azure://%s.blob.core.windows.net/%s/',
            $this->accountName,
            $this->container
        );
    }

    public function getManifestEntries(): array
    {
        if (!$this->isSliced) {
            return [$this->getContainerUrl() . $this->getFilePath()];
        }

        $SASConnectionString = Resources::BLOB_ENDPOINT_NAME .
            '=' .
            'https://' .
            $this->getAccountName() .
            '.' .
            Resources::BLOB_BASE_DNS_NAME .
            ';' .
            Resources::SAS_TOKEN_NAME .
            '=' .
            $this->getSasToken();
        $blobClient = BlobRestProxy::createBlobService(
            $SASConnectionString
        );

        $getResult = $blobClient->getBlob($this->getContainer(), $this->getFilePath());
        $manifest = \GuzzleHttp\json_decode(stream_get_contents($getResult->getContentStream()), true);
        return array_map(function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }
}
