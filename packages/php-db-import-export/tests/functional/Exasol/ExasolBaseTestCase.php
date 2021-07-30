<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
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
    public const TABLE_OUT_CSV_2COLS = 'out.csv_2Cols';
    public const TABLE_OUT_LEMMA = 'out.lemma';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out.no_timestamp_table';
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

    protected function initTable(
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

}
