<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

abstract class BaseFile
{
    /**
     * @var string
     */
    protected $container;

    /**
     * @var string
     */
    protected $sasToken;

    /**
     * @var string
     */
    protected $accountName;

    /**
     * @var string
     */
    protected $filePath;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName
    ) {
        $this->container = $container;
        $this->sasToken = $sasToken;
        $this->accountName = $accountName;
        $this->filePath = $filePath;
    }

    public function getSasToken(): string
    {
        return $this->sasToken;
    }

    public function getContainerUrl(): string
    {
        return sprintf(
            'azure://%s.blob.core.windows.net/%s/',
            $this->accountName,
            $this->container
        );
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }
}
