<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\Middleware;
use Exception;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportFunctional\DebugLogger;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class SnowflakeBaseTestCase extends ImportExportBaseTest
{
    protected const SNFLK_DEST_SCHEMA_NAME = 'in_c_tests';
    protected const SNFLK_SOURCE_SCHEMA_NAME = 'some_tests';
    public const TABLE_ACCOUNTS_3 = 'accounts_3';
    public const TABLE_ACCOUNTS_WITHOUT_TS = 'accounts_without_ts';
    public const TABLE_COLUMN_NAME_ROW_NUMBER = 'column_name_row_number';
    public const TABLE_MULTI_PK = 'multi_pk';
    public const TABLE_MULTI_PK_WITH_TS = 'multi_pk_ts';
    public const TABLE_SINGLE_PK = 'single_pk';
    public const TABLE_OUT_CSV_2COLS = 'out_csv_2Cols';
    public const TABLE_NULL_EMPTY_STRING = 'null-and-empty-string';
    public const TABLE_OUT_CSV_2COLS_WITHOUT_TS = 'out_csv_2Cols_without_ts';
    public const TABLE_OUT_LEMMA = 'out_lemma';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out_no_timestamp_table';
    public const TESTS_PREFIX = 'import_export_test_';

    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSnowflakeConnection();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    protected function getGCSBucketEnvName(): string
    {
        return 'GCS_BUCKET_NAME';
    }

    protected function getSnowflakeConnection(): Connection
    {
        return SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            (string) getenv('SNOWFLAKE_CERT'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
            ],
            $this->getDoctrineLogger(),
        );
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName,
    ): void {
        $this->connection->executeQuery(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
            $id,
            SnowflakeQuote::quote($firstName),
            SnowflakeQuote::quote($lastName),
        ));
    }

    protected function initSingleTable(
        string $schema = self::SNFLK_SOURCE_SCHEMA_NAME,
        string $table = self::TABLE_TABLE,
        ?string $tableTemplate = null,
    ): void {
        if (!$this->schemaExists($schema)) {
            $this->createSchema($schema);
        }

        if ($tableTemplate === null) {
            $tableTemplate = 'CREATE OR REPLACE TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);';
        }
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                $tableTemplate,
                SnowflakeQuote::quoteSingleIdentifier($schema),
                SnowflakeQuote::quoteSingleIdentifier($table),
            ),
        );
    }

    protected function initTable(string $tableName): void
    {
        // TODO - zatim jsou tady Exasol queries
        switch ($tableName) {
            case self::TABLE_OUT_CSV_2COLS_WITHOUT_TS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE TABLE %s.%s (
          "col1" VARCHAR(20000)  ,
          "col2" VARCHAR(20000)  
        );',
                        SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                        SnowflakeQuote::quoteSingleIdentifier($tableName),
                    ),
                );
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE TABLE %s.%s (
          "col1" VARCHAR(20000)  ,
          "col2" VARCHAR(20000)  ,
          "_timestamp" TIMESTAMP
        );',
                        SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                        SnowflakeQuote::quoteSingleIdentifier($tableName),
                    ),
                );

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', CURRENT_TIMESTAMP());',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
          "col1" VARCHAR(2000000) ,
          "col2" VARCHAR(2000000) 
        );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'a\', \'b\');',
                    SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'c\', \'d\');',
                    SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_ACCOUNTS_WITHOUT_TS:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000) CONSTRAINT "accounts_pk" PRIMARY KEY,
                "idTwitter" VARCHAR(2000000) ,
                "name" VARCHAR(2000000) ,
                "import" VARCHAR(2000000) ,
                "isImported" VARCHAR(2000000) ,
                "apiLimitExceededDatetime" VARCHAR(2000000) ,
                "analyzeSentiment" VARCHAR(2000000) ,
                "importKloutScore" VARCHAR(2000000) ,
                "timestamp" VARCHAR(2000000) ,
                "oauthToken" VARCHAR(2000000) ,
                "oauthSecret" VARCHAR(2000000) ,
                "idApp" VARCHAR(2000000)
            ) ',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_NULLIFY:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000)   ,
                "col1" VARCHAR(2000000) ,
                "col2" VARCHAR(2000000) 
            ) ',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_TYPES:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s."types" (
              "charCol"  VARCHAR(2000000) ,
              "numCol"   VARCHAR(2000000) ,
              "floatCol" VARCHAR(2000000) ,
              "boolCol"  VARCHAR(2000000) ,
              "_timestamp" TIMESTAMP
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE  %s."types" (
              "charCol"  VARCHAR(4000) ,
              "numCol" decimal(10,1) ,
              "floatCol" float ,
              "boolCol" tinyint 
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                ));
                $this->connection->executeQuery(sprintf(
                    'INSERT INTO  %s."types" VALUES
              (\'a\', \'10.5\', \'0.3\', 1)
           ;',
                    SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                ));
                break;
            case self::TABLE_COLUMN_NAME_ROW_NUMBER:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
              "id" VARCHAR(4000) ,
              "row_number" VARCHAR(4000) ,
              "_timestamp" TIMESTAMP
           )',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_SINGLE_PK:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000),
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            CONSTRAINT "visitidpk" PRIMARY KEY ("VisitID")
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_MULTI_PK:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000),
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            CONSTRAINT "visit_something_pk" PRIMARY KEY ("VisitID", "Something")
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_MULTI_PK_WITH_TS:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000),
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            "_timestamp" TIMESTAMP,
            CONSTRAINT "triple_pk" PRIMARY KEY ("VisitID", "Value", "MenuItem")
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000) ,
                "idTwitter" VARCHAR(2000000) ,
                "name" VARCHAR(2000000) ,
                "import" VARCHAR(2000000) ,
                "isImported" VARCHAR(2000000) ,
                "apiLimitExceededDatetime" VARCHAR(2000000) ,
                "analyzeSentiment" VARCHAR(2000000) ,
                "importKloutScore" VARCHAR(2000000) ,
                "timestamp" VARCHAR(2000000) ,
                "oauthToken" VARCHAR(2000000) ,
                "oauthSecret" VARCHAR(2000000) ,
                "idApp" VARCHAR(2000000) ,
                "_timestamp" TIMESTAMP,
                CONSTRAINT "accounts_id_pk" PRIMARY KEY ("id")
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_OUT_LEMMA:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
          "ts" VARCHAR(2000000)         ,
          "lemma" VARCHAR(2000000)      ,
          "lemmaIndex" VARCHAR(2000000) ,
                "_timestamp" TIMESTAMP
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                                "column" VARCHAR(2000000)         ,
                                "table" VARCHAR(2000000)      ,
                                "lemmaIndex" VARCHAR(2000000) ,
                "_timestamp" TIMESTAMP
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                                "col1" VARCHAR(2000000)         ,
                                "col2" VARCHAR(2000000)      
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            case self::TABLE_NULL_EMPTY_STRING:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                                "col" BOOLEAN         ,
                                "str" VARCHAR(2000000),
                                "_timestamp" TIMESTAMP
            );',
                    SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ));
                break;
            default:
                throw new Exception("unknown table {$tableName}");
        }
    }

    protected function getSourceSchemaName(): string
    {
        return self::SNFLK_DEST_SCHEMA_NAME
            . '-'
            . getenv('SUITE');
    }

    protected function getDestinationSchemaName(): string
    {
        return self::SNFLK_SOURCE_SCHEMA_NAME
            . '-'
            . getenv('SUITE');
    }

    protected function cleanSchema(string $schemaName): void
    {
        $this->connection->executeQuery(
            sprintf(
                'DROP SCHEMA IF EXISTS %s CASCADE',
                SnowflakeQuote::quoteSingleIdentifier($schemaName),
            ),
        );
    }

    protected function schemaExists(string $schemaName): bool
    {
        return (bool) $this->connection->fetchOne(
            sprintf(
                'SHOW SCHEMAS LIKE %s;',
                SnowflakeQuote::quote($schemaName),
            ),
        );
    }

    public function createSchema(string $schemaName): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                SnowflakeQuote::quoteSingleIdentifier($schemaName),
            ),
        );
    }

    protected function getSnowflakeImportOptions(
        int $skipLines = 1,
        bool $useTimeStamp = true,
    ): SnowflakeImportOptions {
        return new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: $useTimeStamp,
            numberOfIgnoredLines: $skipLines,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertSnowflakeTableEqualsExpected(
        SourceInterface $source,
        SnowflakeTableDefinition $destination,
        SnowflakeImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected',
    ): void {
        $tableColumns = (new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName(),
        ))->getColumnsNames();

        if ($options->useTimestamp()) {
            self::assertContains('_timestamp', $tableColumns);
        } else {
            self::assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $source->getColumnsNames(), true)) {
            $tableColumns = array_filter($tableColumns, static function ($column) {
                return $column !== '_timestamp';
            });
        }

        $tableColumns = array_map(static function ($column) {
            return sprintf('%s', $column);
        }, $tableColumns);

        $sql = sprintf(
            'SELECT %s FROM %s.%s',
            implode(', ', array_map(static function ($item) {
                return SnowflakeQuote::quoteSingleIdentifier($item);
            }, $tableColumns)),
            SnowflakeQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destination->getTableName()),
        );

        $queryResult = array_map(static function ($row) {
            return array_map(static function ($column) {
                return $column;
            }, array_values($row));
        }, $this->connection->fetchAllAssociative($sql));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message,
        );
    }

    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
    ): SnowflakeImportOptions {
        return new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: $skipLines,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
    }

    protected function createNullableGenericColumn(string $columnName): SnowflakeColumn
    {
        $definition = new Snowflake(
            Snowflake::TYPE_VARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => true,
            ],
        );

        return new SnowflakeColumn(
            $columnName,
            $definition,
        );
    }
}
