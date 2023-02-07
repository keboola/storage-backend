<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Exception;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class ExasolBaseTestCase extends ImportExportBaseTest
{
    // TODO exasol cannot have "." in schema name
    protected const EXASOL_DEST_SCHEMA_NAME = 'in_c-tests';
    protected const EXASOL_SOURCE_SCHEMA_NAME = 'some_tests';
    public const TESTS_PREFIX = 'import-export-test_';

    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getExasolConnection();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    private function getExasolConnection(): Connection
    {
        return ExasolConnectionFactory::getConnection(
            (string) getenv('EXASOL_HOST'),
            (string) getenv('EXASOL_USERNAME'),
            (string) getenv('EXASOL_PASSWORD'),
            $this->getDoctrineLogger()
        );
    }

    protected function insertRowToTable(
        string $schemaName,
        string $tableName,
        int $id,
        string $firstName,
        string $lastName
    ): void {
        $this->connection->executeQuery(sprintf(
            'INSERT INTO %s.%s VALUES (%d, %s, %s)',
            ExasolQuote::quoteSingleIdentifier($schemaName),
            ExasolQuote::quoteSingleIdentifier($tableName),
            $id,
            ExasolQuote::quote($firstName),
            ExasolQuote::quote($lastName)
        ));
    }

    protected function initSingleTable(
        string $schema = self::EXASOL_SOURCE_SCHEMA_NAME,
        string $table = self::TABLE_TABLE
    ): void {
        if (!$this->schemaExists($schema)) {
            $this->createSchema($schema);
        }
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                ExasolQuote::quoteSingleIdentifier($schema),
                ExasolQuote::quoteSingleIdentifier($table)
            )
        );
    }

    protected function initTable(string $tableName): void
    {
        switch ($tableName) {
            case self::TABLE_OUT_CSV_2COLS_WITHOUT_TS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE TABLE %s.%s (
          "col1" VARCHAR(20000)  ,
          "col2" VARCHAR(20000)  
        );',
                        ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                        ExasolQuote::quoteSingleIdentifier($tableName)
                    )
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
                        ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                        ExasolQuote::quoteSingleIdentifier($tableName)
                    )
                );

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', NOW());',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
          "col1" VARCHAR(2000000) ,
          "col2" VARCHAR(2000000) 
        );',
                    ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'a\', \'b\');',
                    ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'c\', \'d\');',
                    ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_ACCOUNTS_WITHOUT_TS:
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
                "idApp" VARCHAR(2000000),
                 CONSTRAINT PRIMARY KEY ("id")
            ) ',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_NULLIFY:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000)   ,
                "col1" VARCHAR(2000000) ,
                "col2" VARCHAR(2000000) 
            ) ',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
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
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName())
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE  %s."types" (
              "charCol"  VARCHAR(4000) ,
              "numCol" decimal(10,1) ,
              "floatCol" float ,
              "boolCol" tinyint 
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName())
                ));
                $this->connection->executeQuery(sprintf(
                    'INSERT INTO  %s."types" VALUES
              (\'a\', \'10.5\', \'0.3\', 1)
           ;',
                    ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName())
                ));
                break;
            case self::TABLE_COLUMN_NAME_ROW_NUMBER:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
              "id" VARCHAR(4000) ,
              "row_number" VARCHAR(4000) ,
              "_timestamp" TIMESTAMP
           )',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_SINGLE_PK:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000) ,
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            CONSTRAINT PRIMARY KEY ("VisitID")
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_MULTI_PK:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000) ,
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            CONSTRAINT PRIMARY KEY ("VisitID", "Something")
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
                // table just for EXA because PK cannot have null nor ''
            case self::TABLE_MULTI_PK_WITH_TS:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000) ,
            "Value"     VARCHAR(2000000),
            "MenuItem"  VARCHAR(2000000),
            "Something" VARCHAR(2000000),
            "Other"     VARCHAR(2000000),
            "_timestamp" TIMESTAMP,
            CONSTRAINT PRIMARY KEY ("VisitID", "Value", "MenuItem")
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
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
                CONSTRAINT PRIMARY KEY ("id")
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
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
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
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
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                                "col1" VARCHAR(2000000)         ,
                                "col2" VARCHAR(2000000)      
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            default:
                throw new Exception("unknown table {$tableName}");
        }
    }

    protected function getSourceSchemaName(): string
    {
        return self::EXASOL_DEST_SCHEMA_NAME
            . '-'
            . getenv('SUITE');
    }

    protected function getDestinationSchemaName(): string
    {
        return self::EXASOL_SOURCE_SCHEMA_NAME
            . '-'
            . getenv('SUITE');
    }

    protected function cleanSchema(string $schemaName): void
    {
        if (!$this->schemaExists($schemaName)) {
            return;
        }

        $this->connection->executeQuery(
            sprintf(
                'DROP SCHEMA %s CASCADE',
                ExasolQuote::quoteSingleIdentifier($schemaName)
            )
        );
    }

    protected function schemaExists(string $schemaName): bool
    {
        return (bool) $this->connection->fetchOne(
            sprintf(
                'SELECT "SCHEMA_NAME" FROM "SYS"."EXA_ALL_SCHEMAS" WHERE "SCHEMA_NAME" = %s',
                ExasolQuote::quote($schemaName)
            )
        );
    }

    public function createSchema(string $schemaName): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE SCHEMA %s;',
                ExasolQuote::quoteSingleIdentifier($schemaName)
            )
        );
    }

    protected function getExasolImportOptions(
        int $skipLines = 1,
        bool $useTimeStamp = true
    ): ExasolImportOptions {
        return new ExasolImportOptions(
            [],
            false,
            $useTimeStamp,
            $skipLines
        );
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertExasolTableEqualsExpected(
        SourceInterface $source,
        ExasolTableDefinition $destination,
        ExasolImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new ExasolTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
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
                return ExasolQuote::quoteSingleIdentifier($item);
            }, $tableColumns)),
            ExasolQuote::quoteSingleIdentifier($destination->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destination->getTableName())
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
            $message
        );
    }
}
