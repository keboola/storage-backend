<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Teradata;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class TeradataBaseTestCase extends ImportExportBaseTest
{
    public const TESTS_PREFIX = 'ieLibTest_';
    public const TEST_DATABASE = self::TESTS_PREFIX . 'refTableDatabase';
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'refTab';
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'refView';
    protected const TERADATA_SOURCE_DATABASE_NAME = 'tests_source';
    protected const TERADATA_DESTINATION_DATABASE_NAME = 'tests_destination';
    // TODO move somewhere else
    public const TABLE_ACCOUNTS_3 = 'accounts-3';
    public const TABLE_ACCOUNTS_BEZ_TS = 'accounts-bez-ts';
    public const TABLE_COLUMN_NAME_ROW_NUMBER = 'column-name-row-number';
    public const TABLE_MULTI_PK = 'multi-pk';
    public const TABLE_MULTI_PK_WITH_TS = 'multi-pk_ts';
    public const TABLE_SINGLE_PK = 'single-pk';
    public const TABLE_OUT_CSV_2COLS = 'out_csv_2Cols';
    public const TABLE_OUT_CSV_2COLS_WITHOUT_TS = 'out_csv_2Cols_without_ts';
    public const TABLE_NULLIFY = 'nullify';
    public const TABLE_OUT_LEMMA = 'out_lemma';
    public const TABLE_OUT_NO_TIMESTAMP_TABLE = 'out_no_timestamp_table';
    public const TABLE_TABLE = 'table';
    public const TABLE_TYPES = 'types';
    public const TABLE_TRANSLATIONS = 'transactions';

    protected Connection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getTeradataConnection();
    }

    protected function getSourceDbName(): string
    {
        return self::TERADATA_SOURCE_DATABASE_NAME
            . '-'
            . getenv('SUITE');
    }

    protected function getDestinationDbName(): string
    {
        return self::TERADATA_DESTINATION_DATABASE_NAME
            . '-'
            . getenv('SUITE');
    }

    private function getTeradataConnection(): Connection
    {
        return TeradataConnection::getConnection([
            'host' => (string) getenv('TERADATA_HOST'),
            'user' => (string) getenv('TERADATA_USERNAME'),
            'password' => (string) getenv('TERADATA_PASSWORD'),
            'port' => (int) getenv('TERADATA_PORT'),
            'dbname' => '',
        ]);
    }

    /**
     * @param string[] $columnsNames
     */
    public function getColumnsWithoutTypes(array $columnsNames): ColumnCollection
    {
        $columns = array_map(function ($colName) {
            return new TeradataColumn(
                $colName,
                new Teradata(
                    Teradata::TYPE_VARCHAR,
                    ['length' => 4000]
                )
            );
        }, $columnsNames);
        return new ColumnCollection($columns);
    }

    /**
     * @param string[] $columns
     * @param string[] $pks
     */
    public function getGenericTableDefinition(
        string $schemaName,
        string $tableName,
        array $columns,
        array $pks = []
    ): TeradataTableDefinition {
        return new TeradataTableDefinition(
            $schemaName,
            $tableName,
            false,
            $this->getColumnsWithoutTypes($columns),
            $pks
        );
    }

    protected function cleanDatabase(string $dbname): void
    {
        if (!$this->dbExists($dbname)) {
            return;
        }

        // delete all objects in the DB
        $this->connection->executeQuery(
            sprintf('DELETE DATABASE %s ALL', TeradataQuote::quoteSingleIdentifier($dbname))
        );
        // drop the empty db
        $this->connection->executeQuery(
            sprintf('DROP DATABASE %s', TeradataQuote::quoteSingleIdentifier($dbname))
        );
    }

    public function createDatabase(string $dbName): void
    {
        $this->connection->executeQuery(sprintf('
CREATE DATABASE %s AS
       PERM = 5e7
       SPOOL = 5e7;
       
       ', TeradataQuote::quoteSingleIdentifier($dbName)));
    }

    protected function dbExists(string $dbname): bool
    {
        try {
            $this->connection->executeQuery(sprintf('HELP DATABASE %s', TeradataQuote::quoteSingleIdentifier($dbname)));
            return true;
        } catch (\Doctrine\DBAL\Exception $e) {
            // https://docs.teradata.com/r/GVKfXcemJFkTJh_89R34UQ/j2TdlzqRJ9LpndY3efMdlw
            if (strpos($e->getMessage(), '3802')) {
                return false;
            }
            throw $e;
        }
    }

    protected function initSingleTable(
        string $db = self::TERADATA_SOURCE_DATABASE_NAME,
        string $table = self::TABLE_TABLE
    ): void {
        if (!$this->dbExists($db)) {
            $this->createDatabase($db);
        }
        // char because of Stats test
        $this->connection->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s, NO FALLBACK
            (
"Other"     VARCHAR(50)
    );',
                TeradataQuote::quoteSingleIdentifier($db),
                TeradataQuote::quoteSingleIdentifier($table)
            )
        );
    }

    protected function initTable(string $tableName): void
    {
        switch ($tableName) {
            case self::TABLE_OUT_CSV_2COLS_WITHOUT_TS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s, NO FALLBACK
            (
"VisitID"   VARCHAR(50) CHARACTER SET UNICODE,
"Value"     VARCHAR(50),
"MenuItem"  VARCHAR(50),
"Something" VARCHAR(50),
"Other"     VARCHAR(50),
    )
PRIMARY INDEX ("VisitID");
        );',
                        TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );
                break;
            case self::TABLE_COLUMN_NAME_ROW_NUMBER:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK
            (
              "id" VARCHAR(50) CHARACTER SET UNICODE,
              "row_number" VARCHAR(50) 
           )',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TRANSLATIONS:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK
            (
              "id" INT ,
              "name" VARCHAR(50) CHARACTER SET UNICODE,
              "price" INT ,
              "isDeleted" INT
           )',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
          "col1" VARCHAR(200)  ,
          "col2" VARCHAR(200)  ,
          "_timestamp" TIMESTAMP
        );',
                        TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', NOW());',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
          "col1" VARCHAR(50) CHARACTER SET UNICODE,
          "col2" VARCHAR(50) 
        );',
                    TeradataQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'a\', \'b\');',
                    TeradataQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'c\', \'d\');',
                    TeradataQuote::quoteSingleIdentifier($this->getSourceDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_LEMMA:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
          "ts" VARCHAR(50)         ,
          "lemma" VARCHAR(50)      ,
          "lemmaIndex" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                "id" VARCHAR(50) CHARACTER SET UNICODE,
                "idTwitter" VARCHAR(50) CHARACTER SET UNICODE,
                "name" VARCHAR(100) CHARACTER SET UNICODE,
                "import" VARCHAR(50) CHARACTER SET UNICODE,
                "isImported" VARCHAR(50) CHARACTER SET UNICODE,
                "apiLimitExceededDatetime" VARCHAR(50) CHARACTER SET UNICODE,
                "analyzeSentiment" VARCHAR(50) CHARACTER SET UNICODE,
                "importKloutScore" VARCHAR(50) CHARACTER SET UNICODE,
                "timestamp" VARCHAR(50) CHARACTER SET UNICODE,
                "oauthToken" VARCHAR(50) CHARACTER SET UNICODE,
                "oauthSecret" VARCHAR(50) CHARACTER SET UNICODE,
                "idApp" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            ) PRIMARY INDEX ("id");',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                                "column" VARCHAR(50)         ,
                                "table" VARCHAR(50)      ,
                                "lemmaIndex" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                                "col1" VARCHAR(50)         ,
                                "col2" VARCHAR(50)      
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;

            case self::TABLE_TYPES:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s."types" (
              "charCol"  VARCHAR(50) CHARACTER SET UNICODE,
              "numCol"   VARCHAR(50) CHARACTER SET UNICODE,
              "floatCol" VARCHAR(50) CHARACTER SET UNICODE,
              "boolCol"  VARCHAR(50) CHARACTER SET UNICODE,
              "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName())
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE  %s."types" (
              "charCol"  VARCHAR(50) CHARACTER SET UNICODE,
              "numCol" decimal(10,1) ,
              "floatCol" float ,
              "boolCol" BYTEINT 
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getSourceDbName())
                ));
                $this->connection->executeQuery(sprintf(
                    'INSERT INTO  %s."types" VALUES
              (\'a\', \'10.5\', \'0.3\', 1)
           ;',
                    TeradataQuote::quoteSingleIdentifier($this->getSourceDbName())
                ));
                break;
            default:
                throw new \Exception('unknown table');
        }
    }

    /**
     * @param string[] $convertEmptyValuesToNull
     */
    protected function getImportOptions(
        array $convertEmptyValuesToNull = [],
        bool $isIncremental = false,
        bool $useTimestamp = false,
        int $numberOfIgnoredLines = 0
    ): TeradataImportOptions {
        return
            new TeradataImportOptions(
                (string) getenv('TERADATA_HOST'),
                (string) getenv('TERADATA_USERNAME'),
                (string) getenv('TERADATA_PASSWORD'),
                (int) getenv('TERADATA_PORT'),
                $convertEmptyValuesToNull,
                $isIncremental,
                $useTimestamp,
                $numberOfIgnoredLines,
            );
    }

    protected function getSimpleImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
        bool $useTimestamp = true
    ): TeradataImportOptions {
        return
            new TeradataImportOptions(
                (string) getenv('TERADATA_HOST'),
                (string) getenv('TERADATA_USERNAME'),
                (string) getenv('TERADATA_PASSWORD'),
                (int) getenv('TERADATA_PORT'),
                [],
                false,
                $useTimestamp,
                $skipLines,
            );
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertTeradataTableEqualsExpected(
        SourceInterface $source,
        TeradataTableDefinition $destination,
        TeradataImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new TeradataTableReflection(
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
                return TeradataQuote::quoteSingleIdentifier($item);
            }, $tableColumns)),
            TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destination->getTableName())
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

    protected function tearDown(): void
    {
        $this->cleanDatabase($this->getDestinationDbName());
        $this->cleanDatabase($this->getSourceDbName());
        parent::tearDown();
    }
}
