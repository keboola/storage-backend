<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS;

use Google\Cloud\Storage\StorageClient;
use Keboola\Db\ImportExport\Storage\DestinationFileInterface;
use Keboola\FileStorage\Gcs\GcsProvider;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\Path\RelativePathInterface;

class DestinationFile extends BaseFile implements DestinationFileInterface
{
    /**
     * @var array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * }
     */
    private array $credentials;

    /** @param array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * } $credentials
     */
    public function __construct(
        string $bucket,
        string $filePath,
        string $storageIntegrationName,
        array $credentials
    ) {
        parent::__construct($bucket, $filePath, $storageIntegrationName);
        $this->credentials = $credentials;
    }

    public function getRelativePath(): RelativePathInterface
    {
        return RelativePath::createFromRootAndPath(
            new GcsProvider(),
            $this->bucket,
            $this->getFilePath(),
        );
    }

    public function getClient(): StorageClient
    {
        return new StorageClient(['keyFile' => $this->credentials]);
    }
}
