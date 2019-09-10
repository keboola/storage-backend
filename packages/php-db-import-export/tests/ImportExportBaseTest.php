<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use PHPUnit\Framework\TestCase;

abstract class ImportExportBaseTest extends TestCase
{
    protected const SNOWFLAKE_SCHEMA_NAME = 'testing-schema';

    /** @var Connection */
    protected $connection;

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSnowflakeConnection();
        $this->initSchemaDb($this->connection);
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->connection);
    }

    protected function assertTableEqualsFiles(string $tableName, array $files, string $sortKey, string $message): void
    {
        $filesContent = [];
        $filesHeader = [];

        foreach ($files as $file) {
            $csvFile = new CsvFile($file);
            $csvFileRows = [];
            foreach ($csvFile as $row) {
                $csvFileRows[] = $row;
            }

            if (empty($filesHeader)) {
                $filesHeader = array_shift($csvFileRows);
            } else {
                $this->assertSame(
                    $filesHeader,
                    array_shift($csvFileRows),
                    'Provided files have incosistent headers'
                );
            }
            foreach ($csvFileRows as $fileRow) {
                $filesContent[] = array_combine($filesHeader, $fileRow);
            }
        }

        $queryResult = $this->connection->fetchAll('SELECT * FROM ' . $this->connection->quoteIdentifier($tableName));
        $this->assertArrayEqualsSorted(
            $filesContent,
            $queryResult,
            $sortKey,
            $message
        );

        $this->assertSame($filesHeader, array_keys(reset($queryResult)));
    }

    private function assertArrayEqualsSorted(
        array $expected,
        array $actual,
        string $sortKey,
        string $message = ''
    ): void {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] === $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        $this->assertEquals($expected, $actual, $message);
    }

    protected function createTableInSnowflake(
        Connection $connection,
        string $tableName,
        array $columns
    ): void {
        $connection->query(sprintf('USE SCHEMA %s', $connection->quoteIdentifier(self::SNOWFLAKE_SCHEMA_NAME)));
        $connection->query(sprintf('DROP TABLE IF EXISTS %s', $connection->quoteIdentifier($tableName)));
        $columnQuery = array_map(function (string $column) use ($connection) {
            return $connection->quoteIdentifier($column) . ' VARCHAR()';
        }, $columns);
        $createQuery = sprintf(
            'CREATE TABLE %s (%s)',
            $connection->quoteIdentifier($tableName),
            implode(',', $columnQuery)
        );
        $connection->query($createQuery);
    }

    private function getSnowflakeConnection(): Connection
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
        $connection->query(
            sprintf(
                'USE DATABASE %s',
                $connection->quoteIdentifier((string) getenv('SNOWFLAKE_DATABASE'))
            )
        );
        $connection->query(
            sprintf(
                'USE WAREHOUSE %s',
                $connection->quoteIdentifier((string) getenv('SNOWFLAKE_WAREHOUSE'))
            )
        );
        return $connection;
    }

    protected function getCredentialsForAzureContainer(string $container): string
    {
        $sasHelper = new BlobSharedAccessSignatureHelper(
            (string) getenv('ABS_ACCOUNT_NAME'),
            (string) getenv('ABS_ACCOUNT_KEY')
        );
        $expirationDate = (new DateTime())->modify('+1hour');
        return $sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            $container,
            'rwl',
            $expirationDate,
            (new DateTime())
        );
    }

    private function initSchemaDb(Connection $connection): void
    {
        $connection->query(
            sprintf(
                'DROP SCHEMA IF EXISTS %s',
                $connection->quoteIdentifier(self::SNOWFLAKE_SCHEMA_NAME)
            )
        );
        $connection->query(
            sprintf(
                'CREATE SCHEMA %s',
                $connection->quoteIdentifier(self::SNOWFLAKE_SCHEMA_NAME)
            )
        );
    }
}
