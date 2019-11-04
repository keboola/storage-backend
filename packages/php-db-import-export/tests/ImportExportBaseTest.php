<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport;

use DateTime;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Result;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\SourceStorage;

abstract class ImportExportBaseTest extends TestCase
{
    protected const DATA_DIR = __DIR__ . '/data/';
    protected const SNOWFLAKE_SCHEMA_NAME = 'testing-schema';

    /** @var Connection */
    protected $connection;

    protected function assertTableEqualsFiles(
        string $tableName,
        array $files,
        string $sortKey,
        string $message
    ): void {
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

    protected function createABSSourceInstance(
        string $file,
        bool $isSliced = false
    ): SourceStorage\ABS\Source {
        return new SourceStorage\ABS\Source(
            (string) getenv('ABS_CONTAINER_NAME'),
            $file,
            $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
            (string) getenv('ABS_ACCOUNT_NAME'),
            new CsvFile($file), //TODO: create file inside or use only CSV file
            $isSliced
        );
    }

    protected function getCredentialsForAzureContainer(
        string $container
    ): string {
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

    protected function createTableInSnowflake(
        Connection $connection,
        ImportOptions $options,
        array $primaryKeys = []
    ): void {
        $connection->query(sprintf('USE SCHEMA %s', $connection->quoteIdentifier(self::SNOWFLAKE_SCHEMA_NAME)));
        $connection->query(sprintf('DROP TABLE IF EXISTS %s', $connection->quoteIdentifier($options->getTableName())));
        $columnQuery = array_map(function (string $column) use ($connection) {
            return $connection->quoteIdentifier($column) . ' VARCHAR()';
        }, $options->getColumns());
        $primaryKeysSql = '';
        if (!empty($primaryKeys)) {
            $quotedPrimaryKeys = array_map(function (string $column): string {
                return QuoteHelper::quoteIdentifier($column);
            }, $primaryKeys);

            $primaryKeysSql = sprintf(
                ', PRIMARY KEY (%s)',
                implode(', ', $quotedPrimaryKeys)
            );
        }
        $createQuery = sprintf(
            'CREATE TABLE %s (%s %s)',
            $connection->quoteIdentifier($options->getTableName()),
            implode(',', $columnQuery),
            $primaryKeysSql
        );
        $connection->query($createQuery);
    }


    protected function importFileToSnowflake(string $file, string $tableName): Result
    {
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            $tableName,
            [],
            $csvFile->getHeader(),
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );

        $this->createTableInSnowflake(
            $this->connection,
            $importOptions
        );
        return (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance($file)
        );
    }

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSnowflakeConnection();
        $this->initSchemaDb($this->connection);
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

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->connection);
    }
}
