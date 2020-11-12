<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

class SourceFile extends BaseFile implements SourceInterface
{
    /** @var bool */
    private $isSliced;

    /** @var CsvOptions */
    private $csvOptions;

    /** @var string[] */
    private $columnsNames;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = []
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->columnsNames = $columnsNames;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getCsvOptions(): CsvOptions
    {
        return $this->csvOptions;
    }

    public function getManifestEntries(
        string $protocol = self::PROTOCOL_AZURE
    ): array {
        $blobClient = $this->getBlobClient();

        if (!$this->isSliced) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobClient->getBlob($this->container, $this->filePath);
            } catch (ServiceException $e) {
                throw new Exception('Load error: ' . $e->getErrorText(), Exception::MANDATORY_FILE_NOT_FOUND, $e);
            }

            return [$this->getContainerUrl($protocol) . $this->filePath];
        }

        try {
            $manifestBlob = $blobClient->getBlob($this->container, $this->filePath);
        } catch (ServiceException $e) {
            throw new Exception(
                'Load error: manifest file was not found.',
                Exception::MANDATORY_FILE_NOT_FOUND,
                $e
            );
        }
        $manifest = \GuzzleHttp\json_decode(stream_get_contents($manifestBlob->getContentStream()), true);
        $entries = array_map(function (array $entry) {
            return $entry['url'];
        }, $manifest['entries']);

        return $this->transformManifestEntries($entries, $protocol, $blobClient);
    }

    protected function getBlobClient(): BlobRestProxy
    {
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
        $blobClient->pushMiddleware(RetryFactory::createRetryMiddleware());
        return $blobClient;
    }

    protected function transformManifestEntries(
        array $entries,
        string $protocol,
        BlobRestProxy $blobClient
    ): array {
        return array_map(function ($entryUrl) use ($protocol, $blobClient) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobPath = explode(sprintf('blob.core.windows.net/%s/', $this->container), $entryUrl)[1];
                $blobClient->getBlob($this->container, $blobPath);
            } catch (ServiceException $e) {
                throw new Exception('Load error: ' . $e->getErrorText(), Exception::MANDATORY_FILE_NOT_FOUND, $e);
            }

            switch ($protocol) {
                case self::PROTOCOL_AZURE:
                    // snowflake needs protocol in files to be azure://
                    return str_replace('https://', 'azure://', $entryUrl);
                case self::PROTOCOL_HTTPS:
                    // synapse needs protocol in files to be https://
                    return str_replace('azure://', 'https://', $entryUrl);
            }
        }, $entries);
    }
}
