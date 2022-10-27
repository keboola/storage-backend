<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use LogicException;
use PHPUnit\Framework\TestCase;

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

    public function createSchema(string $schemaName): void
    {
        $this->bqClient->createDataset($schemaName);
    }

    protected function schemaExists(string $schemaName): bool
    {
        return $this->bqClient->dataset($schemaName)->exists();
    }

    protected function cleanSchema(string $schemaName): void
    {
        if (!$this->schemaExists($schemaName)) {
            return;
        }

        $this->bqClient->dataset($schemaName)->delete(['deleteContents' => true]);
    }

    /**
     * Get credentials from envs
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
        bool $createNewSchema = true
    ): void {
        if ($createNewSchema) {
            $this->createSchema($schema);
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
                BigqueryQuote::quoteSingleIdentifier($table)
            )
        );

        $this->bqClient->runQuery($query);
    }

    private function getBigqueryClient(): BigQueryClient
    {
        return new BigQueryClient([
            'keyFile' => $this->getCredentials(),
        ]);
    }
}
