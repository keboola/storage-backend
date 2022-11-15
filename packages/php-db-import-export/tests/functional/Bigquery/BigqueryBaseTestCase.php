<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class BigqueryBaseTestCase extends ImportExportBaseTest
{
    public const TESTS_PREFIX = 'ieLibTest_';
    public const TEST_DATABASE = self::TESTS_PREFIX . 'refTableDatabase';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    protected const BIGQUERY_SOURCE_DATABASE_NAME = 'tests_source';
    protected const BIGQUERY_DESTINATION_DATABASE_NAME = 'tests_destination';
    public const TABLE_TRANSLATIONS = 'transactions';

    public const TABLE_TABLE = 'test_table';

    protected BigQueryClient $bqClient;

    protected function setUp(): void
    {
        parent::setUp();
        $this->bqClient = $this->getBigqueryConnection();
    }

    protected function cleanDatabase(string $dbname): void
    {
        if (!$this->datasetExists($dbname)) {
            return;
        }

        $this->bqClient->dataset($dbname)->delete(['deleteContents' => true]);
    }

    protected function createDatabase(string $dbName): void
    {
        $this->bqClient->createDataset($dbName);
    }

    private function getBigqueryConnection(): BigQueryClient
    {
        return new BigQueryClient([
            'keyFile' => $this->getBqCredentials(),
        ]);
    }

    protected function datasetExists(string $datasetName): bool
    {
        return $this->bqClient->dataset($datasetName)->exists();
    }

    /**
     * @param string[] $convertEmptyValuesToNull
     */
    protected function getImportOptions(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0
    ): ImportOptions {
        return
            new ImportOptions(
                $convertEmptyValuesToNull,
                $isIncremental,
                $useTimestamp,
                $numberOfIgnoredLines,
            );
    }

    protected function getSourceDbName(): string
    {
        /** @var string $suitePrefixEnv */
        $suitePrefixEnv = getenv('SUITE');
        return self::BIGQUERY_SOURCE_DATABASE_NAME
            . '_'
            . str_replace('-', '_', $suitePrefixEnv);
    }

    protected function getDestinationDbName(): string
    {
        /** @var string $suitePrefixEnv */
        $suitePrefixEnv = getenv('SUITE');
        return self::BIGQUERY_DESTINATION_DATABASE_NAME
            . '_'
            . str_replace('-', '_', $suitePrefixEnv);
    }

    protected function initTable(string $tableName, string $dbName = ''): void
    {
        if ($dbName === '') {
            $dbName = $this->getDestinationDbName();
        }

        switch ($tableName) {
            case self::TABLE_TRANSLATIONS:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s
            (
              `id` INT64 ,
              `name` STRING(50),
              `price` DECIMAL,
              `isDeleted` INT64
           )',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->bqClient->runQuery($this->bqClient->query(
                    sprintf(
                        'CREATE TABLE %s.%s (
          `col1` STRING(500),
          `col2` STRING(500),
          `_timestamp` TIMESTAMP
        );',
                        BigqueryQuote::quoteSingleIdentifier($dbName),
                        BigqueryQuote::quoteSingleIdentifier($tableName)
                    )
                ));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', CURRENT_TIMESTAMP());',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
          `col1` STRING(50),
          `col2` STRING(50) 
        );',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'a\', \'b\');',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));

                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'INSERT INTO %s.%s VALUES (\'c\', \'d\');',
                    BigqueryQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_LEMMA:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
          `ts` STRING(50),
          `lemma` STRING(50),
          `lemmaIndex` STRING(50),
                `_timestamp` TIMESTAMP
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                `id` STRING(50),
                `idTwitter` STRING(50),
                `name` STRING(100),
                `import` STRING(50),
                `isImported` STRING(50),
                `apiLimitExceededDatetime` STRING(50),
                `analyzeSentiment` STRING(50),
                `importKloutScore` STRING(50),
                `timestamp` STRING(50),
                `oauthToken` STRING(50),
                `oauthSecret` STRING(50),
                `idApp` STRING(50),
                `_timestamp` TIMESTAMP
            )',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_TABLE:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                                `column` STRING(50)         ,
                                `table` STRING(50)      ,
                                `lemmaIndex` STRING(50),
                `_timestamp` TIMESTAMP
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->bqClient->runQuery($this->bqClient->query(sprintf(
                    'CREATE TABLE %s.%s (
                                `col1` STRING(50)         ,
                                `col2` STRING(50)      
            );',
                    BigqueryQuote::quoteSingleIdentifier($dbName),
                    BigqueryQuote::quoteSingleIdentifier($tableName)
                )));
                break;
            default:
                throw new Exception('unknown table');
        }
    }

    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): ImportOptions {
        return new ImportOptions(
            [],
            false,
            true,
            $skipLines
        );
    }

    protected function initSingleTable(
        string $db = self::BIGQUERY_SOURCE_DATABASE_NAME,
        string $table = self::TABLE_TABLE
    ): void {
        if (!$this->datasetExists($db)) {
            $this->createDatabase($db);
        }
        // char because of Stats test
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s
            (
`Other`     STRING(50)
    );',
                BigqueryQuote::quoteSingleIdentifier($db),
                BigqueryQuote::quoteSingleIdentifier($table)
            )
        ));
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
    private function getBqCredentials(): array
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
        $credentials = json_decode((string) getenv('BQ_KEY_FILE'), true, 512, JSON_THROW_ON_ERROR);
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

    protected function getGCSBucketEnvName(): string
    {
        return 'BQ_BUCKET_NAME';
    }

    protected function getGCSCredentials(): array
    {
        return $this->getBqCredentials();
    }
}
