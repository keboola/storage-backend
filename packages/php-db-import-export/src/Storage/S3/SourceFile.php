<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Aws\Exception\AwsException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SourceFile implements SourceInterface
{
    /**
     * @var bool
     */
    private $isSliced;

    /**
     * @var CsvOptions
     */
    private $csvOptions;

    /**
     * @var string
     */
    private $key;

    /**
     * @var string
     */
    private $secret;

    /**
     * @var string
     */
    private $region;

    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var string
     */
    protected $filePath;

    /** @var string[] */
    private $columnsNames;

    /** @var string[]|null */
    private $primaryKeysNames;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $key,
        string $secret,
        string $region,
        string $bucket,
        string $filePath,
        CsvOptions $csvOptions,
        bool $isSliced,
        array $columnsNames = [],
        ?array $primaryKeysNames = null
    ) {
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->key = $key;
        $this->secret = $secret;
        $this->region = $region;
        $this->bucket = $bucket;
        $this->filePath = $filePath;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    protected function getClient(): \Aws\S3\S3Client
    {
        return new \Aws\S3\S3Client([
            'credentials' => [
                'key' => $this->key,
                'secret' => $this->secret,
            ],
            'region' => $this->region,
            'version' => '2006-03-01',
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

    public function getKey(): string
    {
        return $this->key;
    }

    /**
     * @return string[]
     */
    public function getManifestEntries(): array
    {
        if (!$this->isSliced) {
            return [$this->getS3Prefix() . '/' . $this->filePath];
        }

        $client = $this->getClient();

        try {
            $response = $client->getObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->filePath, '/'),
            ]);
        } catch (AwsException $e) {
            throw new Exception('Load error: ' . $e->getMessage(), Exception::MANDATORY_FILE_NOT_FOUND, $e);
        }

        $manifest = json_decode((string) $response['Body'], true);
        return array_map(static function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }

    public function getS3Prefix(): string
    {
        return sprintf('s3://%s', $this->bucket);
    }

    /**
     * @return string[]|null
     */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    public function getRegion(): string
    {
        return $this->region;
    }

    public function getSecret(): string
    {
        return $this->secret;
    }
}
