<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS;

use Google\Cloud\Storage\StorageClient;

abstract class BaseFile
{
    protected string $filePath;

    protected string $bucket;

    protected string $storageIntegrationName;

    public function __construct(
        string $bucket,
        string $filePath,
        string $storageIntegrationName,
    ) {
        $this->filePath = $filePath;
        $this->bucket = $bucket;
        $this->storageIntegrationName = $storageIntegrationName;
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }

    public function getBucket(): string
    {
        return $this->bucket;
    }

    public function getStorageIntegrationName(): string
    {
        return $this->storageIntegrationName;
    }

    public function getGcsPrefix(): string
    {
        return sprintf('gcs://%s', $this->bucket);
    }
}
