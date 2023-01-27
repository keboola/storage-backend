<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage;

trait S3SourceTrait
{
    protected function createDummyS3SourceInstance(
        string $file,
        bool $isSliced = false
    ): Storage\S3\SourceFile {
        return new Storage\S3\SourceFile(
            's3Key',
            's3Secret',
            'eu-central-1',
            'myBucket',
            $file,
            new CsvOptions(),
            $isSliced,
            []
        );
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     */
    protected function createS3SourceInstance(
        string $filePath,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ): Storage\S3\SourceFile {
        return $this->createS3SourceInstanceFromCsv(
            $filePath,
            new CsvOptions(),
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys
        );
    }

    /**
     * filePath is expected without AWS_S3_KEY
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     */
    protected function createS3SourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ): Storage\S3\SourceFile {
        if ($isDirectory) {
            $class = Storage\S3\SourceDirectory::class;
        } else {
            $class = Storage\S3\SourceFile::class;
        }
        $key = (string) getenv('AWS_S3_KEY');
        return new $class(
            (string) getenv('AWS_ACCESS_KEY_ID'),
            (string) getenv('AWS_SECRET_ACCESS_KEY'),
            (string) getenv('AWS_REGION'),
            (string) getenv('AWS_S3_BUCKET'),
            $key ? ($key . '/' . $filePath) : $filePath,
            $options,
            $isSliced,
            $columns,
            $primaryKeys
        );
    }
}
