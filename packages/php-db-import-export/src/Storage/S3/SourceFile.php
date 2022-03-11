<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Aws\Exception\AwsException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SourceFile extends BaseFile implements SourceInterface
{
    /**
     * @var bool
     */
    private $isSliced;

    /**
     * @var CsvOptions
     */
    private $csvOptions;

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
        parent::__construct(
            $key,
            $secret,
            $region,
            $bucket,
            $filePath
        );
        $this->isSliced = $isSliced;
        $this->csvOptions = $csvOptions;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
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
     * @return string
     * @throws \Exception
     */
    public function getFileName(): string
    {
        if ($this->isSliced) {
            throw new \Exception('Not supported getFileName for sliced files.');
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
     * @return string
     * @throws \Exception
     */
    public function getPrefix(): string
    {
        $prefix = $this->filePath;
        if (($prefixLength = strrpos($prefix, '/')) !== false) {
            // include / at the end
            return substr($prefix, 0, $prefixLength + 1);
        }
        return '';
    }
}
