<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DBALException;
use Exception;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Teradata\TeradataExportOptions;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class TeradataBaseTestCase extends ImportExportBaseTest
{
    public const BIGGER_TABLE = 'big_table';
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

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
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
        $db = TeradataConnection::getConnection([
            'host' => (string) getenv('TERADATA_HOST'),
            'user' => (string) getenv('TERADATA_USERNAME'),
            'password' => (string) getenv('TERADATA_PASSWORD'),
            'port' => (int) getenv('TERADATA_PORT'),
            'dbname' => '',
        ]);

        if ((string) getenv('TERADATA_DATABASE') === '') {
            throw new Exception('Variable "TERADATA_DATABASE" is missing.');
        }
        $db->executeStatement(sprintf(
            'SET SESSION DATABASE %s;',
            TeradataQuote::quoteSingleIdentifier((string) getenv('TERADATA_DATABASE'))
        ));

        return $db;
    }

    /**
     * @return TeradataImportOptions::CSV_ADAPTER_*
     */
    public function getCsvAdapter(): string
    {
        $adapter = (string) getenv('TERADATA_CSV_ADAPTER');
        switch ($adapter) {
            case TeradataImportOptions::CSV_ADAPTER_TPT:
                return TeradataImportOptions::CSV_ADAPTER_TPT;
        }
        return TeradataImportOptions::CSV_ADAPTER_TPT;
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

    public function createDatabase(string $dbName, string $perm = '5e7', string $spool = '5e7'): void
    {
        // 2e7 = 20MB
        $this->connection->executeQuery(sprintf('
CREATE DATABASE %s AS
       PERM = %s
       SPOOL = %s;
       ', TeradataQuote::quoteSingleIdentifier($dbName), $perm, $spool));
    }

    protected function dbExists(string $dbname): bool
    {
        try {
            $this->connection->executeQuery(sprintf('HELP DATABASE %s', TeradataQuote::quoteSingleIdentifier($dbname)));
            return true;
        } catch (DBALException $e) {
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
                'CREATE MULTISET TABLE %s.%s
            (
"Other"     VARCHAR(50)
    );',
                TeradataQuote::quoteSingleIdentifier($db),
                TeradataQuote::quoteSingleIdentifier($table)
            )
        );
    }

    protected function initTable(string $tableName, string $dbName = ''): void
    {
        if ($dbName === '') {
            $dbName = $this->getDestinationDbName();
        }

        switch ($tableName) {
            case self::BIGGER_TABLE:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s
            (
"FID" VARCHAR(500) CHARACTER SET UNICODE,
"NAZEV" VARCHAR(500) CHARACTER SET UNICODE,
"Y" VARCHAR(500) CHARACTER SET UNICODE,
"X" VARCHAR(500) CHARACTER SET UNICODE,
"KONTAKT" VARCHAR(500) CHARACTER SET UNICODE,
"SUBKATEGORIE" VARCHAR(500) CHARACTER SET UNICODE,
"KATEGORIE" VARCHAR(500) CHARACTER SET UNICODE,
"Column6" VARCHAR(500) CHARACTER SET UNICODE,
"Column7" VARCHAR(500) CHARACTER SET UNICODE,
"Column8" VARCHAR(500) CHARACTER SET UNICODE,
"Column9" VARCHAR(500) CHARACTER SET UNICODE,
"GlobalID" VARCHAR(500) CHARACTER SET UNICODE
    );',
                        TeradataQuote::quoteSingleIdentifier($dbName),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );

                break;
            case self::TABLE_OUT_CSV_2COLS_WITHOUT_TS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s
            (
"VisitID"   VARCHAR(50) CHARACTER SET UNICODE,
"Value"     VARCHAR(50),
"MenuItem"  VARCHAR(50),
"Something" VARCHAR(50),
"Other"     VARCHAR(50),
    )
PRIMARY INDEX ("VisitID");',
                        TeradataQuote::quoteSingleIdentifier($dbName),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );
                break;
            case self::TABLE_COLUMN_NAME_ROW_NUMBER:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s
            (
              "id" VARCHAR(50) CHARACTER SET UNICODE,
              "row_number" VARCHAR(50) 
           )',
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TRANSLATIONS:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s
            (
              "id" INT ,
              "name" VARCHAR(50) CHARACTER SET UNICODE,
              "price" INT ,
              "isDeleted" INT
           )',
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s (
          "col1" VARCHAR(500)  ,
          "col2" VARCHAR(500)  ,
          "_timestamp" TIMESTAMP
        );',
                        TeradataQuote::quoteSingleIdentifier($dbName),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );

                $this->connection->executeQuery(sprintf(
                    'INSERT INTO %s.%s VALUES (\'x\', \'y\', NOW());',
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));

                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s (
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
                    'CREATE MULTISET TABLE %s.%s (
          "ts" VARCHAR(50)         ,
          "lemma" VARCHAR(50)      ,
          "lemmaIndex" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s (
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
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s (
                                "column" VARCHAR(50)         ,
                                "table" VARCHAR(50)      ,
                                "lemmaIndex" VARCHAR(50) CHARACTER SET UNICODE,
                "_timestamp" TIMESTAMP
            );',
                    TeradataQuote::quoteSingleIdentifier($dbName),
                    TeradataQuote::quoteSingleIdentifier($tableName)
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->executeQuery(sprintf(
                    'CREATE MULTISET TABLE %s.%s (
                                "col1" VARCHAR(50)         ,
                                "col2" VARCHAR(50)      
            );',
                    TeradataQuote::quoteSingleIdentifier($dbName),
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
                    TeradataQuote::quoteSingleIdentifier($dbName)
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
                throw new Exception('unknown table');
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
                $this->getCsvAdapter()
            );
    }

    protected function getExportOptions(
        bool $isCompressed = false,
        string $bufferSize = TeradataExportOptions::DEFAULT_BUFFER_SIZE,
        string $maxObjectSize = TeradataExportOptions::DEFAULT_MAX_OBJECT_SIZE,
        bool $dontSplitRows = TeradataExportOptions::DEFAULT_SPLIT_ROWS,
        bool $singlePartFile = TeradataExportOptions::DEFAULT_SINGLE_PART_FILE
    ): TeradataExportOptions {
        return
            new TeradataExportOptions(
                (string) getenv('TERADATA_HOST'),
                (string) getenv('TERADATA_USERNAME'),
                (string) getenv('TERADATA_PASSWORD'),
                (int) getenv('TERADATA_PORT'),
                $isCompressed,
                $bufferSize,
                $maxObjectSize,
                $dontSplitRows,
                $singlePartFile,
                ExportOptions::MANIFEST_AUTOGENERATED
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
                $this->getCsvAdapter()
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
}
