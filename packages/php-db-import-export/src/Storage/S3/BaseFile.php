<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

abstract class BaseFile
{
    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var string
     */
    protected $filePath;

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

    public function __construct(
        string $key,
        string $secret,
        string $region,
        string $bucket,
        string $filePath
    ) {
        $this->key = $key;
        $this->secret = $secret;
        $this->region = $region;
        $this->bucket = $bucket;
        $this->filePath = $filePath;
    }

    protected function getClient(): \Aws\S3\S3Client
    {
        return new \Aws\S3\S3Client([
            'credentials' => [
                'key' => $this->key,
                'secret' => $this->secret,
            ],
            'retries' => 40,
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 120,
            ],
            'region' => $this->region,
            'version' => '2006-03-01',
        ]);
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getS3Prefix(): string
    {
        return sprintf('s3://%s', $this->bucket);
    }

    public function getRegion(): string
    {
        return $this->region;
    }

    public function getSecret(): string
    {
        return $this->secret;
    }

    public function getFilePath(): string
    {
        return $this->filePath;
    }

    public function getBucket(): string
    {
        return $this->bucket;
    }

    public function getBucketURL(): string
    {
        return sprintf('https://%s.s3.%s.amazonaws.com', $this->bucket, $this->region);
    }
}
