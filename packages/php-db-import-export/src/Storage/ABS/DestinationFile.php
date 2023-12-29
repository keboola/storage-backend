<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Storage\DestinationFileInterface;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\FileStorage\Path\RelativePath;
use Keboola\FileStorage\Path\RelativePathInterface;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class DestinationFile extends BaseFile implements DestinationFileInterface
{
    private ?string $blobMasterKey = null;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        ?string $blobMasterKey = null,
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->blobMasterKey = $blobMasterKey;
    }

    public function getBlobMasterKey(): ?string
    {
        return $this->blobMasterKey;
    }

    public function getRelativePath(): RelativePathInterface
    {
        return RelativePath::createFromRootAndPath(
            new AbsProvider(),
            $this->container,
            $this->getFilePath(),
        );
    }

    public function getClient(): BlobRestProxy
    {
        $SASConnectionString = sprintf(
            '%s=https://%s.%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            $this->accountName,
            Resources::BLOB_BASE_DNS_NAME,
            Resources::SAS_TOKEN_NAME,
            $this->sasToken,
        );

        return ClientFactory::createClientFromConnectionString(
            $SASConnectionString,
        );
    }
}
