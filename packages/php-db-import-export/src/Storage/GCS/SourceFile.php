<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\GCS;

use Exception as InternalException;
use Google\Cloud\Storage\StorageClient;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Throwable;

class SourceFile extends BaseFile implements SourceInterface
{
    public const PROTOCOL_GCS = 'GCS';
    public const PROTOCOL_GS = 'GS';

    private bool $isSliced;

    private CsvOptions $csvOptions;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames = null;

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
     * client_x509_cert_url: string
     * }
     */
    private array $credentials;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     * @param array{
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
     * } $credentials $credentials
     */
    public function __construct(
        string $bucket,
        string $filePath,
        string $storageIntegrationName,
        array $credentials,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = [],
        ?array $primaryKeysNames = null
    ) {
        parent::__construct(
            $bucket,
            $filePath,
            $storageIntegrationName
        );
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
        $this->credentials = $credentials;
    }

    protected function getClient(): StorageClient
    {
        return new StorageClient([
            'keyFile' => $this->credentials,
            'debug' => true,
        ]);
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
    public function getManifestEntries(string $protocol = self::PROTOCOL_GCS): array
    {
        if (!$this->isSliced) {
            return [$this->getGcsPrefix() . '/' . $this->filePath];
        }

        $client = $this->getClient();

        try {
            $bucket = $client->bucket($this->bucket);
            $response = $bucket->object($this->filePath);
            /** @var array{entries:array{url:string}} $manifest */
            $manifest = json_decode($response->downloadAsString(), true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable $e) {
            throw new Exception('Load error: ' . $e->getMessage(), Exception::MANDATORY_FILE_NOT_FOUND, $e);
        }

        $entries = array_map(static fn(array $entry) => $entry['url'], $manifest['entries']);

        return $this->transformManifestEntries($entries, $protocol);
    }

    public function isSliced(): bool
    {
        return $this->isSliced;
    }

    /**
     * @return string[]|null
     */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
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
        $prefix = $this->filePath;
        $prefixLength = strrpos($prefix, '/');
        if ($prefixLength !== false) {
            // include / at the end
            return substr($prefix, 0, $prefixLength + 1);
        }
        return '';
    }

    /**
     * @param string[] $entries
     * @return string[]
     */
    protected function transformManifestEntries(
        array $entries,
        string $protocol
    ): array {
        return array_map(static function ($entryUrl) use ($protocol): string {
            switch ($protocol) {
                case self::PROTOCOL_GCS:
                    // snowflake needs protocol in files to be gcs://
                    return str_replace('gs://', 'gcs://', $entryUrl);
                case self::PROTOCOL_GS:
                    // whole world expects protocol to be gs://
                    return str_replace('gcs://', 'gs://', $entryUrl);
            }
            throw new InternalException(sprintf('Unknown protocol %s', $protocol));
        }, $entries);
    }
}
