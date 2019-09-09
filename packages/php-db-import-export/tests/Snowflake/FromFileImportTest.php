<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport\Snowflake;

use DateTime;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake;
use Keboola\Db\ImportExport\ImportOptions;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\File;

class FromFileImportTest extends TestCase
{
    private const SNOWFLAKE_SCHEMA_NAME = 'testing-schema';

    public function testImportFile(): void
    {
        $prefix = __DIR__ . '/../data/';
        $file = $prefix . 'file.csv';
        $connection = $this->getSnowflakeConnection();
        $this->initSchemaDb($connection);
        $snowflake = new Snowflake($connection);
        $this->createTableInSnowflake(
            $connection,
            'testingTable',
            self::SNOWFLAKE_SCHEMA_NAME,
            (new CsvFile($file))->getHeader()
        );

        $importOptions = new ImportOptions(self::SNOWFLAKE_SCHEMA_NAME, 'testingTable');

        $snowflake->importTableFromFile(
            $importOptions,
            new File\Azure(
                (string) getenv('ABS_CONTAINER_NAME'),
                strtr($file, [$prefix => '']),
                $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
                (string) getenv('ABS_ACCOUNT_NAME'),
                new CsvFile(strtr($file, [$prefix => ''])),
                false
            )
        );

        $this->assertSame(
            [
                ['a' => 'a', 'b' => 'b', 'c' => 'c'],
                ['a' => '1', 'b' => '2', 'c' => '3'],
            ],
            $connection->fetchAll(
                sprintf('SELECT * FROM %s', $connection->quoteIdentifier($importOptions->getTableName()))
            )
        );
    }

    private function createTableInSnowflake(
        Connection $connection,
        string $tableName,
        string $schemaName,
        array $columns
    ): void {
        $connection->query(sprintf('USE SCHEMA %s', $connection->quoteIdentifier($schemaName)));
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

    private function getCredentialsForAzureContainer(string $container): string
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
