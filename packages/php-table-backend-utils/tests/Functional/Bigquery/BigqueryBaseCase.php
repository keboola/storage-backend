<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use GuzzleHttp\Client;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientHandler;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Keboola\TableBackendUtils\Connection\Bigquery\Retry;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class BigqueryBaseCase extends TestCase
{
    public const TESTS_PREFIX = 'utilsTest_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'refTableSchema';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';

    protected BigQueryClient $bqClient;

    protected function setUp(): void
    {
        parent::setUp();
        $this->bqClient = $this->getBigqueryClient();
    }

    public function createDataset(string $schemaName): void
    {
        $this->bqClient->createDataset($schemaName);
    }

    protected function datasetExists(string $schemaName): bool
    {
        return $this->bqClient->dataset($schemaName)->exists();
    }

    protected function cleanDataset(string $schemaName): void
    {
        if (!$this->datasetExists($schemaName)) {
            return;
        }

        $this->bqClient->dataset($schemaName)->delete(['deleteContents' => true]);
    }

    /**
     * Get credentials from envs
     *
     * @return array<string, string>
     */
    protected function getCredentials(): array
    {
        $keyFile = getenv('BQ_KEY_FILE');
        if ($keyFile === false) {
            throw new LogicException('Env "BQ_KEY_FILE" is empty');
        }

        /** @var array<string, string> $credentials */
        $credentials = json_decode($keyFile, true, 512, JSON_THROW_ON_ERROR);
        assert($credentials !== false);
        return $credentials;
    }

    protected function initTable(
        string $schema = self::TEST_SCHEMA,
        string $table = self::TABLE_GENERIC,
        bool $createNewSchema = true,
    ): void {
        if ($createNewSchema) {
            $this->createDataset($schema);
        }

        // char because of Stats test
        $query = $this->bqClient->query(
            sprintf(
                'CREATE OR REPLACE TABLE %s.%s (
            `id` INTEGER,
    `first_name` STRING(100),
    `last_name` STRING(100)
);',
                BigqueryQuote::quoteSingleIdentifier($schema),
                BigqueryQuote::quoteSingleIdentifier($table),
            ),
        );

        $this->bqClient->runQuery($query);
    }

    private function getBigqueryClient(): BigQueryClient
    {
        return new BigQueryClientWrapper(
            [
                'keyFile' => $this->getCredentials(),
                'httpHandler' => new BigQueryClientHandler(new Client()),
                'restRetryFunction' => Retry::getRestRetryFunction(new NullLogger(), true),
            ],
            'e2e-utils-lib',
        );
    }

    public function getDatasetName(): string
    {
        return getenv('TEST_PREFIX') . self::TEST_SCHEMA;
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName,
    ): void {
        $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            BigqueryQuote::quoteSingleIdentifier($schemaName),
            BigqueryQuote::quoteSingleIdentifier($tableName),
            $id,
            BigqueryQuote::quote($firstName),
            BigqueryQuote::quote($lastName),
        )));
    }
}
