<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportCommon;

use Exception;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage;

trait GCSSourceTrait
{
    protected function getGCSBucketEnvName(): string
    {
        throw new Exception('Method "getGCSBucketEnvName" must be overridden in you test case.');
    }

    protected function createDummyGCSSourceInstance(
        string $file,
        bool $isSliced = false,
    ): Storage\GCS\SourceFile {
        return new Storage\GCS\SourceFile(
            'gcsBucket',
            $file,
            'integration',
            [
                'type' => '',
                'project_id' => '',
                'private_key_id' => '',
                'private_key' => '',
                'client_email' => '',
                'client_id' => '',
                'auth_uri' => '',
                'token_uri' => '',
                'auth_provider_x509_cert_url' => '',
                'client_x509_cert_url' => '',
            ],
            new CsvOptions(),
            $isSliced,
            [],
        );
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     */
    protected function createGCSSourceInstance(
        string $filePath,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null,
    ): Storage\GCS\SourceFile {
        return $this->createGCSSourceInstanceFromCsv(
            $filePath,
            new CsvOptions(),
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys,
        );
    }

    /**
     * filePath is expected without AWS_GCS_KEY
     *
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     */
    protected function createGCSSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null,
    ): Storage\GCS\SourceFile {
        if ($isDirectory) {
            throw new Exception('Directory not supported for GCS');
        }

        return new Storage\GCS\SourceFile(
            (string) getenv($this->getGCSBucketEnvName()),
            $filePath,
            (string) getenv('GCS_INTEGRATION_NAME'),
            $this->getGCSCredentials(),
            $options,
            $isSliced,
            $columns,
            $primaryKeys,
        );
    }

    /**
     * @return array{
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
     * }
     */
    protected function getGCSCredentials(): array
    {
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
         * client_x509_cert_url: string,
         * }
         */
        $credentials = json_decode((string) getenv('GCS_CREDENTIALS'), true, 512, JSON_THROW_ON_ERROR);
        assert(array_key_exists('type', $credentials));
        assert(array_key_exists('project_id', $credentials));
        assert(array_key_exists('private_key_id', $credentials));
        assert(array_key_exists('private_key', $credentials));
        assert(array_key_exists('client_email', $credentials));
        assert(array_key_exists('client_id', $credentials));
        assert(array_key_exists('auth_uri', $credentials));
        assert(array_key_exists('token_uri', $credentials));
        assert(array_key_exists('auth_provider_x509_cert_url', $credentials));
        return $credentials;
    }
}
