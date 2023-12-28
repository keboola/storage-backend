<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Backend\Synapse\SynapseExportOptions;

abstract class BaseFile
{
    public const PROTOCOL_AZURE = 'azure';
    public const PROTOCOL_HTTPS = 'https';

    protected string $container;

    protected string $sasToken;

    protected string $accountName;

    protected string $filePath;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
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
            $this->container,
        );
    }

    public function getPolyBaseUrl(string $credentialsType): string
    {
        if ($credentialsType === SynapseExportOptions::CREDENTIALS_MANAGED_IDENTITY) {
            return sprintf(
                'abfss://%s@%s.dfs.core.windows.net/',
                $this->container,
                $this->accountName,
            );
        }
        return sprintf(
            'wasbs://%s@%s.blob.core.windows.net/',
            $this->container,
            $this->accountName,
        );
    }
    public function getContainer(): string
    {
        return $this->container;
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }

    public function getAccountName(): string
    {
        return $this->accountName;
    }
}
