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
    protected const TERADATA_DESTINATION_DATABASE_NAME = 'tests_source';
    // TODO move somewhere else
    protected const EXASOL_DEST_SCHEMA_NAME = 'in_c-tests';
    protected const EXASOL_SOURCE_SCHEMA_NAME = 'some_tests';
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
        $this->connection->executeQuery(sprintf('DELETE DATABASE %s ALL', $dbname));
        // drop the empty db
        $this->connection->executeQuery(sprintf('DROP DATABASE %s', $dbname));
    }

    public function createDatabase(string $dbName): void
    {
        $this->connection->executeQuery(sprintf('
CREATE DATABASE %s AS
       PERM = 5e6;
       ', $dbName));
    }

    protected function dbExists(string $dbname): bool
    {
        try {
            $this->connection->executeQuery(sprintf('HELP DATABASE %s', $dbname));
            return true;
        } catch (\Doctrine\DBAL\Exception $e) {
            return false;
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
            $destination->getSchemaName(),
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
            $destination->getSchemaName(),
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

    protected function initTable(string $tableName): void
    {
        switch ($tableName) {
            case self::TABLE_OUT_CSV_2COLS_WITHOUT_TS:
                $this->connection->executeQuery(
                    sprintf(
                        'CREATE MULTISET TABLE %s.%s, NO FALLBACK
            (
"VisitID"   VARCHAR(2000000) ,
"Value"     VARCHAR(2000000),
"MenuItem"  VARCHAR(2000000),
"Something" VARCHAR(2000000),
"Other"     VARCHAR(2000000),
    )
PRIMARY INDEX ("VisitID");
        );',
                        TeradataQuote::quoteSingleIdentifier($this->getDestinationDbName()),
                        TeradataQuote::quoteSingleIdentifier($tableName)
                    )
                );
                break;
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
}
