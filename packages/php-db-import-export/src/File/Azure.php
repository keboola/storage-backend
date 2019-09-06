<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\File;

class Azure
{
    /** @var string */
    private $container;

    /** @var string */
    private $filePath;

    /** @var string */
    private $sasToken;

    /** @var string */
    private $accountName;

    public function __construct(string $container, string $filePath, string $sasToken, string $accountName)
    {
        $this->container = $container;
        $this->filePath = $filePath;
        $this->sasToken = $sasToken;
        $this->accountName = $accountName;
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
}
