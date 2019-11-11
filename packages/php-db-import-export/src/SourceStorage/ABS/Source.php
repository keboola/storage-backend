<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\SourceStorage\ABS;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\SourceStorage\NoBackendAdapterException;
use Keboola\Db\ImportExport\SourceStorage\SourceInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class Source implements SourceInterface
{
    /**
     * @var string
     */
    private $container;

    /**
     * @var string
     */
    private $sasToken;

    /**
     * @var string
     */
    private $accountName;

    /**
     * @var string
     */
    private $filePath;

    /**
     * @var bool
     */
    private $isSliced;

    /**
     * @var CsvFile
     */
    private $csvFile;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvFile $csvFile,
        bool $isSliced
    ) {
        $this->container = $container;
        $this->sasToken = $sasToken;
        $this->accountName = $accountName;
        $this->filePath = $filePath;
        $this->isSliced = $isSliced;
        $this->csvFile = $csvFile;
    }

    public function getBackendImportAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        switch (true) {
            case $importer instanceof SnowflakeImporter:
                return new SnowflakeAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getCsvFile(): CsvFile
    {
        return $this->csvFile;
    }

    public function getManifestEntries(): array
    {
        if (!$this->isSliced) {
            return [$this->getContainerUrl() . $this->filePath];
        }

        $SASConnectionString = sprintf(
            '%s=https://%s.%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            $this->accountName,
            Resources::BLOB_BASE_DNS_NAME,
            Resources::SAS_TOKEN_NAME,
            $this->sasToken
        );

        $blobClient = BlobRestProxy::createBlobService(
            $SASConnectionString
        );

        $getResult = $blobClient->getBlob($this->container, $this->filePath);
        $manifest = \GuzzleHttp\json_decode(stream_get_contents($getResult->getContentStream()), true);
        return array_map(function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }

    public function getContainerUrl(): string
    {
        return sprintf(
            'azure://%s.blob.core.windows.net/%s/',
            $this->accountName,
            $this->container
        );
    }

    public function getSasToken(): string
    {
        return $this->sasToken;
    }
}
