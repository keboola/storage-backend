<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

abstract class BaseFile
{
    public const PROTOCOL_AZURE = 'azure';
    public const PROTOCOL_HTTPS = 'https';

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

    /**
     * Snowflake won't import files if protocol is other than azure://
     * Synapse won't import files if protocol is other than https://
     */
    public function getContainerUrl(string $protocol): string
    {
        return sprintf(
            '%s://%s.blob.core.windows.net/%s/',
            $protocol,
            $this->accountName,
            $this->container
        );
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }
}
