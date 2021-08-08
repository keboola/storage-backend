<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\SqlCommandBuilder;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnection;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class ExasolBaseTestCase extends ImportExportBaseTest
{
    // TODO exasol cannot have "." in schema name
    protected const EXASOL_DEST_SCHEMA_NAME = 'in_c-tests';
    protected const EXASOL_SOURCE_SCHEMA_NAME = 'some_tests';
    public const TABLE_ACCOUNTS_3 = 'accounts-3';
    public const TABLE_ACCOUNTS_BEZ_TS = 'accounts-bez-ts';
    public const TABLE_COLUMN_NAME_ROW_NUMBER = 'column-name-row-number';
    public const TABLE_MULTI_PK = 'multi-pk';
    public const TABLE_SINGLE_PK = 'single-pk';
    public const TABLE_OUT_CSV_2COLS = 'out_csv_2Cols';
    public const TABLE_NULLIFY = 'nullify';
    public const TABLE_OUT_LEMMA = 'out.lemma';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out_no_timestamp_table';
    public const TABLE_TABLE = 'table';
    public const TABLE_TYPES = 'types';
    public const TESTS_PREFIX = 'import-export-test_';

    /** @var Connection */
    protected $connection;

    /** @var SqlCommandBuilder */
    protected $qb;

    /** @var OraclePlatform|AbstractPlatform */
    protected $platform;

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
        return ExasolConnection::getConnection(
            (string) getenv('EXASOL_HOST'),
            (string) getenv('EXASOL_USERNAME'),
            (string) getenv('EXASOL_PASSWORD')
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
        $now = (new \DateTime('now', new \DateTimeZone('UTC')))->format('Y-m-d H:i:s');

        switch ($tableName) {
            case self::TABLE_OUT_CSV_2COLS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE TABLE %s.%s (
          "col1" VARCHAR(20000)  DEFAULT \'\' NOT NULL,
          "col2" VARCHAR(20000)  DEFAULT \'\' NOT NULL
        );',
                        ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                        ExasolQuote::quoteSingleIdentifier($tableName)
                    )
                );

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\');',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                    //                    $now
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
          "col1" NVARCHAR(4000) DEFAULT \'\' NOT NULL,
          "col2" NVARCHAR(4000) DEFAULT \'\' NOT NULL
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
            case self::TABLE_ACCOUNTS_BEZ_TS:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000) NOT NULL,
                "idTwitter" VARCHAR(2000000) NOT NULL,
                "name" VARCHAR(2000000) NOT NULL,
                "import" VARCHAR(2000000) NOT NULL,
                "isImported" VARCHAR(2000000) NOT NULL,
                "apiLimitExceededDatetime" VARCHAR(2000000) NOT NULL,
                "analyzeSentiment" VARCHAR(2000000) NOT NULL,
                "importKloutScore" VARCHAR(2000000) NOT NULL,
                "timestamp" VARCHAR(2000000) NOT NULL,
                "oauthToken" VARCHAR(2000000) NOT NULL,
                "oauthSecret" VARCHAR(2000000) NOT NULL,
                "idApp" VARCHAR(2000000) NOT NULL
            ) ',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_NULLIFY:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
                "id" VARCHAR(2000000)   NOT NULL,
                "col1" VARCHAR(2000000) NOT NULL,
                "col2" VARCHAR(2000000) NOT NULL
            ) ',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TYPES:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s."types" (
              "charCol"  VARCHAR(2000000) NOT NULL,
              "numCol"   VARCHAR(2000000) NOT NULL,
              "floatCol" VARCHAR(2000000) NOT NULL,
              "boolCol"  VARCHAR(2000000) NOT NULL
            );',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName())
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE  %s."types" (
              "charCol"  VARCHAR(4000) NOT NULL,
              "numCol" decimal(10,1) NOT NULL,
              "floatCol" float NOT NULL,
              "boolCol" tinyint NOT NULL
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
              "id" VARCHAR(4000) NOT NULL,
              "row_number" VARCHAR(4000) NOT NULL,
              "_timestamp" TIMESTAMP
           )',
                    ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                    ExasolQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_SINGLE_PK:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s.%s (
            "VisitID"   VARCHAR(2000000) NOT NULL,
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
            "VisitID"   VARCHAR(2000000) NOT NULL,
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
            default:
                throw new \Exception("unknown table {$tableName}");
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
        int $skipLines = 0
    ): ExasolImportOptions {
        return new ExasolImportOptions(
            [],
            false,
            false,
            $skipLines
        );
    }
}
