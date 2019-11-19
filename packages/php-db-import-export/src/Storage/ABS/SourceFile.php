<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class SourceFile extends BaseFile implements SourceInterface
{
    /**
     * @var bool
     */
    private $isSliced;

    /**
     * @var CsvOptions
     */
    private $csvOptions;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvOptions $csvOptions,
        bool $isSliced
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
    }

    public function getBackendImportAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        switch (true) {
            case $importer instanceof SnowflakeImporter:
                return new SnowflakeImportAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getCsvOptions(): CsvOptions
    {
        return $this->csvOptions;
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
}
