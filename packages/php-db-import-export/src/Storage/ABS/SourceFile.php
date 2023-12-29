<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Exception as InternalException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Keboola\Db\ImportExport\Storage\ManifestNotFoundException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\FileStorage\Abs\AbsProvider;
use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\FileStorage\Abs\LineEnding\LineEndingDetector;
use Keboola\FileStorage\FileNotFoundException as FIleStorageFileNotFoundException;
use Keboola\FileStorage\LineEnding\StringLineEndingDetectorHelper;
use Keboola\FileStorage\Path\RelativePath;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use function GuzzleHttp\json_decode;

class SourceFile extends BaseFile implements SourceInterface
{
    private bool $isSliced;

    private CsvOptions $csvOptions;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames;

    private ?string $blobMasterKey;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = [],
        ?array $primaryKeysNames = null,
        ?string $blobMasterKey = null,
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
        $this->blobMasterKey = $blobMasterKey;
    }

    protected function getBlobPath(string $entryUrl): string
    {
        return explode(sprintf('blob.core.windows.net/%s/', $this->container), $entryUrl)[1];
    }

    /**
     * @return mixed
     * @throws Exception
     */
    private function downloadAndParseManifest(BlobRestProxy $blobClient)
    {
        try {
            $manifestBlob = $blobClient->getBlob($this->container, $this->filePath);
        } catch (ServiceException $e) {
            throw new ManifestNotFoundException($e);
        }
        return json_decode(stream_get_contents($manifestBlob->getContentStream()), true);
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

    /**
     * @return string[]
     */
    public function getManifestEntries(
        string $protocol = self::PROTOCOL_AZURE,
    ): array {
        $blobClient = $this->getBlobClient();

        if (!$this->isSliced) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobClient->getBlob($this->container, $this->filePath);
            } catch (ServiceException $e) {
                throw FileNotFoundException::createFromServiceException($e);
            }

            return [$this->getContainerUrl($protocol) . $this->filePath];
        }

        $manifest = $this->downloadAndParseManifest($blobClient);
        $entries = array_map(function (array $entry) {
            return $entry['url'];
        }, $manifest['entries']);

        return $this->transformManifestEntries($entries, $protocol, $blobClient);
    }

    public function getBlobMasterKey(): ?string
    {
        return $this->blobMasterKey;
    }

    protected function getBlobClient(): BlobRestProxy
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

    /**
     * @param string[] $entries
     * @return string[]
     */
    protected function transformManifestEntries(
        array $entries,
        string $protocol,
        BlobRestProxy $blobClient,
    ): array {
        return array_map(function ($entryUrl) use ($protocol, $blobClient) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobPath = $this->getBlobPath($entryUrl);
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
     * @return string[]|null
     */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /**
     * @return StringLineEndingDetectorHelper::EOL_*
     */
    public function getLineEnding(): string
    {
        $client = $this->getBlobClient();
        $detector = LineEndingDetector::createForClient($client);

        if (!$this->isSliced) {
            $file = RelativePath::createFromRootAndPath(new AbsProvider(), $this->container, $this->filePath);
        } else {
            $manifest = $this->downloadAndParseManifest($client);
            if (count($manifest['entries']) === 0) {
                return StringLineEndingDetectorHelper::EOL_UNIX;
            }
            $blobPath = $this->getBlobPath($manifest['entries'][0]['url']);
            $file = RelativePath::createFromRootAndPath(new AbsProvider(), $this->container, $blobPath);
        }

        try {
            return $detector->getLineEnding($file);
        } catch (FIleStorageFileNotFoundException $e) {
            throw FileNotFoundException::createFromFileNotFoundException($e);
        }
    }

    public function isSliced(): bool
    {
        return $this->isSliced;
    }

    /**
     * from path data/shared/file.csv to file.csv
     *
     * @throws InternalException
     */
    public function getFileName(): string
    {
        if ($this->isSliced) {
            throw new InternalException('Not supported getFileName for sliced files.');
        }
        $fileName = $this->filePath;
        if (strrpos($fileName, '/') !== false) {
            // there is dir in the path
            return substr($fileName, strrpos($fileName, '/') + 1);
        }
        // there is no dir in the path, just the filename
        return $fileName;
    }

    /**
     * from path data/shared/file.csv to data/shared/
     *
     * @throws InternalException
     */
    public function getPrefix(): string
    {
        $filePath = $this->getFilePath();
        // SourceDirectory returns fileName as directory/file.csv
        // but SourceFile returns azure://myaccount...windows.net/bucket/directory/file.csv
        $filePath = str_replace(($this->getContainerUrl(BaseFile::PROTOCOL_AZURE)), '', $filePath);

        $exploded = explode('/', $filePath);
        // get all the parts of exploded path but without the last thing - the filename
        $prefix = implode('/', array_slice($exploded, 0, -1));
        // prefix should end with / but only if it exists
        return $prefix ? ($prefix . '/') : '';
    }
}
