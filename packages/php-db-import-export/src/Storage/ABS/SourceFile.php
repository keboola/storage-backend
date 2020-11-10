<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
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

    /** @var string */
    private $type;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = [],
        string $type = self::TYPE_FILE
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->columnsNames = $columnsNames;
        $this->type = $type;
    }

    public function getCsvOptions(): CsvOptions
    {
        return $this->csvOptions;
    }

    public function getManifestEntries(string $protocol = self::PROTOCOL_AZURE): array
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
        $blobClient->pushMiddleware(RetryMiddlewareFactory::create());

        if ($this->type === self::TYPE_FILE && !$this->isSliced) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobClient->getBlob($this->container, $this->filePath);
            } catch (ServiceException $e) {
                throw new Exception('Load error: ' . $e->getErrorText(), Exception::MANDATORY_FILE_NOT_FOUND, $e);
            }

            return [$this->getContainerUrl($protocol) . $this->filePath];
        }

        switch ($this->type) {
            case self::TYPE_FILE:
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
                break;
            case self::TYPE_FOLDER:
                $path = $this->filePath;
                if (substr($path, -1) !== '/') {
                    // add trailing slash if not set to list only blobs in folder
                    $path .= '/';
                }
                $options = new ListBlobsOptions();
                $options->setPrefix($path);
                $result = $blobClient->listBlobs($this->container, $options);
                $entries = array_map(function (Blob $blob) {
                    return $blob->getUrl();
                }, $result->getBlobs());
                break;
            default:
                throw new \LogicException(sprintf('Unknown source type "%s".', $this->type));
        }

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

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }
}
