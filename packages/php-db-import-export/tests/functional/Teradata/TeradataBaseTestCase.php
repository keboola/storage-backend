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
        return new TeradataTableDefinition (
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
        $this->connection->executeQuery(sprintf('DELETE DATABASE %s ALL', TeradataQuote::quoteSingleIdentifier($dbname)));
        // drop the empty db
        $this->connection->executeQuery(sprintf('DROP DATABASE %s', TeradataQuote::quoteSingleIdentifier($dbname)));
    }

    public function createDatabase(string $dbName): void
    {
        $this->connection->executeQuery(sprintf('
CREATE DATABASE %s AS
       PERM = 5e6;
       ', TeradataQuote::quoteSingleIdentifier($dbName)));
    }

    protected function dbExists(string $dbname): bool
    {
        try {
            $this->connection->executeQuery(sprintf('HELP DATABASE %s', TeradataQuote::quoteSingleIdentifier($dbname)));
            return true;
        } catch (\Doctrine\DBAL\Exception $e) {
            // https://docs.teradata.com/r/GVKfXcemJFkTJh_89R34UQ/j2TdlzqRJ9LpndY3efMdlw
            if (strpos($e->getMessage(), "3802")) {
                return false;
            }
            throw $e;
        }
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertSynapseTableEqualsExpected(
        SourceInterface $source,
        TeradataTableDefinition $destination,
        ImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new TeradataTableReflection(
            $this->connection,
            $destination->getDbName(),
            $destination->getTableName()
        ))->getColumnsNames();

        if ($options->useTimestamp()) {
            self::assertContains('_timestamp', $tableColumns);
        } else {
            self::assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $source->getColumnsNames())) {
            $tableColumns = array_filter($tableColumns, function ($column) {
                return $column !== '_timestamp';
            });
        }

        $tableColumns = array_map(function ($column) {
            return sprintf('[%s]', $column);
        }, $tableColumns);

        $sql = sprintf(
            'SELECT %s FROM [%s].[%s]',
            implode(', ', $tableColumns),
            $destination->getDbName(),
            $destination->getTableName()
        );

        $queryResult = array_map(function ($row) {
            return array_map(function ($column) {
                return $column;
            }, array_values($row));
        }, $this->connection->fetchAll($sql));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message
        );
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
"Other"     VARCHAR(4000)
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
"VisitID"   VARCHAR(4000) ,
"Value"     VARCHAR(4000),
"MenuItem"  VARCHAR(4000),
"Something" VARCHAR(4000),
"Other"     VARCHAR(4000),
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
              "id" VARCHAR(4000) ,
              "row_number" VARCHAR(4000) 
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
              "name" VARCHAR(4000) ,
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
          "col1" VARCHAR(20000)  ,
          "col2" VARCHAR(20000)  ,
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
          "col1" VARCHAR(4000) ,
          "col2" VARCHAR(4000) 
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
          "ts" VARCHAR(4000)         ,
          "lemma" VARCHAR(4000)      ,
          "lemmaIndex" VARCHAR(4000) ,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                "id" VARCHAR(4000) ,
                "idTwitter" VARCHAR(4000) ,
                "name" VARCHAR(4000) ,
                "import" VARCHAR(4000) ,
                "isImported" VARCHAR(4000) ,
                "apiLimitExceededDatetime" VARCHAR(4000) ,
                "analyzeSentiment" VARCHAR(4000) ,
                "importKloutScore" VARCHAR(4000) ,
                "timestamp" VARCHAR(4000) ,
                "oauthToken" VARCHAR(4000) ,
                "oauthSecret" VARCHAR(4000) ,
                "idApp" VARCHAR(4000) ,
                "_timestamp" TIMESTAMP
            ) PRIMARY INDEX ("id");',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                                "column" VARCHAR(4000)         ,
                                "table" VARCHAR(4000)      ,
                                "lemmaIndex" VARCHAR(4000) ,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s, NO FALLBACK (
                                "col1" VARCHAR(4000)         ,
                                "col2" VARCHAR(4000)      
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;

            case self::TABLE_TYPES:
                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE %s."types" (
              "charCol"  VARCHAR(4000) ,
              "numCol"   VARCHAR(4000) ,
              "floatCol" VARCHAR(4000) ,
              "boolCol"  VARCHAR(4000) ,
              "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName())
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE TABLE  %s."types" (
              "charCol"  VARCHAR(4000) ,
              "numCol" decimal(10,1) ,
              "floatCol" float ,
              "boolCol" tinyint 
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
        int $skipLines = 0
    ): TeradataImportOptions {
        return
            new TeradataImportOptions(
                (string) getenv('TERADATA_HOST'),
                (string) getenv('TERADATA_USERNAME'),
                (string) getenv('TERADATA_PASSWORD'),
                (int) getenv('TERADATA_PORT'),
                [],
                false,
                false,
                $skipLines,
            );
    }
}
