<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Datatype\Definition\Synapse;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

class SynapseBaseTestCase extends ImportExportBaseTest
{
    protected const SYNAPSE_DEST_SCHEMA_NAME = 'in.c-tests';
    protected const SYNAPSE_SOURCE_SCHEMA_NAME = 'some.tests';

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

    protected Connection $connection;

    protected SqlBuilder $qb;

    protected function dropAllWithinSchema(string $schema): void
    {
        $tables = $this->connection->fetchAllAssociative(
            <<< EOT
SELECT name
FROM sys.tables
WHERE schema_name(schema_id) = '$schema'
order by name;
EOT
        );

        foreach ($tables as $table) {
            $this->connection->exec(
                $this->qb->getDropCommand($schema, $table['name'])
            );
        }

        /** @var array<array{name:string}> $schemas */
        $schemas = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT name FROM sys.schemas WHERE name = \'%s\'',
                $schema
            )
        );

        foreach ($schemas as $item) {
            $this->connection->exec(
                sprintf(
                    'DROP SCHEMA %s',
                    SynapseQuote::quoteSingleIdentifier($item['name'])
                )
            );
        }
    }

    /**
     * @param string[] $tables
     */
    protected function initTables(array $tables): void
    {
        foreach ($tables as $table) {
            $this->initTable($table);
        }
    }

    protected function initTable(string $tableName): void
    {
        $currentDate = new DateTime('now', new DateTimeZone('UTC'));
        $now = $currentDate->format('Y-m-d H:i:s');

        $tableDistribution = 'ROUND_ROBIN';
        if (getenv('TABLE_DISTRIBUTION') !== false) {
            $tableDistribution = getenv('TABLE_DISTRIBUTION');
        }

        switch ($tableName) {
            case self::TABLE_OUT_LEMMA:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[out.lemma] (
          [ts] nvarchar(4000) NOT NULL DEFAULT \'\',
          [lemma] nvarchar(4000) NOT NULL DEFAULT \'\',
          [lemmaIndex] nvarchar(4000) NOT NULL DEFAULT \'\',
          [_timestamp] datetime2
        );',
                    $this->getDestinationSchemaName()
                ));
                break;
            case self::TABLE_OUT_CSV_2COLS:
                $this->connection->exec(sprintf('CREATE TABLE [%s].[out.csv_2Cols] (
          [col1] nvarchar(4000) NOT NULL DEFAULT \'\',
          [col2] nvarchar(4000) NOT NULL DEFAULT \'\',
          [_timestamp] datetime2
        );', $this->getDestinationSchemaName()));

                $this->connection->exec(sprintf(
                    'INSERT INTO [%s].[out.csv_2Cols] VALUES
                  (\'x\', \'y\', \'%s\');',
                    $this->getDestinationSchemaName(),
                    $now
                ));

                $this->connection->exec(sprintf('CREATE TABLE [%s].[out.csv_2Cols] (
          [col1] nvarchar(4000) NOT NULL DEFAULT \'\',
          [col2] nvarchar(4000) NOT NULL DEFAULT \'\'
        );', $this->getSourceSchemaName()));

                $this->connection->exec(sprintf('INSERT INTO [%s].[out.csv_2Cols] VALUES
                (\'a\', \'b\');
        ', $this->getSourceSchemaName()));
                $this->connection->exec(sprintf('INSERT INTO [%s].[out.csv_2Cols] VALUES
                (\'c\', \'d\');
        ', $this->getSourceSchemaName()));
                break;
            case self::TABLE_ACCOUNTS_3:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[accounts-3] (
                [id] nvarchar(4000) NOT NULL,
                [idTwitter] nvarchar(4000) NOT NULL,
                [name] nvarchar(4000) NOT NULL,
                [import] nvarchar(4000) NOT NULL,
                [isImported] nvarchar(4000) NOT NULL,
                [apiLimitExceededDatetime] nvarchar(4000) NOT NULL,
                [analyzeSentiment] nvarchar(4000) NOT NULL,
                [importKloutScore] nvarchar(4000) NOT NULL,
                [timestamp] nvarchar(4000) NOT NULL,
                [oauthToken] nvarchar(4000) NOT NULL,
                [oauthSecret] nvarchar(4000) NOT NULL,
                [idApp] nvarchar(4000) NOT NULL,
                [_timestamp] datetime2,
                PRIMARY KEY NONCLUSTERED("id") NOT ENFORCED
            ) WITH (DISTRIBUTION=%s)',
                    $this->getDestinationSchemaName(),
                    $tableDistribution === 'HASH' ? 'HASH([id])' : $tableDistribution
                ));
                break;
            case self::TABLE_ACCOUNTS_BEZ_TS:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[accounts-bez-ts] (
                [id] nvarchar(4000) NOT NULL,
                [idTwitter] nvarchar(4000) NOT NULL,
                [name] nvarchar(4000) NOT NULL,
                [import] nvarchar(4000) NOT NULL,
                [isImported] nvarchar(4000) NOT NULL,
                [apiLimitExceededDatetime] nvarchar(4000) NOT NULL,
                [analyzeSentiment] nvarchar(4000) NOT NULL,
                [importKloutScore] nvarchar(4000) NOT NULL,
                [timestamp] nvarchar(4000) NOT NULL,
                [oauthToken] nvarchar(4000) NOT NULL,
                [oauthSecret] nvarchar(4000) NOT NULL,
                [idApp] nvarchar(4000) NOT NULL,
                PRIMARY KEY NONCLUSTERED("id") NOT ENFORCED
            ) WITH (DISTRIBUTION=%s)',
                    $this->getDestinationSchemaName(),
                    $tableDistribution === 'HASH' ? 'HASH([id])' : $tableDistribution
                ));
                break;
            case self::TABLE_TABLE:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[table] (
              [column]  nvarchar(4000) NOT NULL DEFAULT \'\',
              [table] nvarchar(4000) NOT NULL DEFAULT \'\',
              [_timestamp] datetime2
            );',
                    $this->getDestinationSchemaName()
                ));
                break;
            case self::TABLE_TYPES:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[types] (
              [charCol]  nvarchar(4000) NOT NULL,
              [numCol] nvarchar(4000) NOT NULL,
              [floatCol] nvarchar(4000) NOT NULL,
              [boolCol] nvarchar(4000) NOT NULL,
              [_timestamp] datetime2
            );',
                    $this->getDestinationSchemaName()
                ));

                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[types] (
              [charCol]  nvarchar(4000) NOT NULL,
              [numCol] decimal(10,1) NOT NULL,
              [floatCol] float NOT NULL,
              [boolCol] tinyint NOT NULL
            );',
                    $this->getSourceSchemaName()
                ));
                $this->connection->exec(sprintf(
                    'INSERT INTO [%s].[types] VALUES
              (\'a\', \'10.5\', \'0.3\', 1)
           ;',
                    $this->getSourceSchemaName()
                ));
                break;
            case self::TABLE_OUT_NO_TIMESTAMP_TABLE:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[out.no_timestamp_table] (
              [col1] nvarchar(4000) NOT NULL DEFAULT \'\',
              [col2] nvarchar(4000) NOT NULL DEFAULT \'\'
            );',
                    $this->getDestinationSchemaName()
                ));
                break;
            case self::TABLE_COLUMN_NAME_ROW_NUMBER:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[column-name-row-number] (
              [id] nvarchar(4000) NOT NULL,
              [row_number] nvarchar(4000) NOT NULL,
              [_timestamp] datetime2,
                PRIMARY KEY NONCLUSTERED("id") NOT ENFORCED
           ) WITH (DISTRIBUTION=%s)',
                    $this->getDestinationSchemaName(),
                    $tableDistribution === 'HASH' ? 'HASH([id])' : $tableDistribution
                ));
                break;
            case self::TABLE_MULTI_PK:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[multi-pk] (
            [VisitID] nvarchar(4000) NOT NULL DEFAULT \'\',
            [Value] nvarchar(4000) NOT NULL DEFAULT \'\',
            [MenuItem] nvarchar(4000) NOT NULL DEFAULT \'\',
            [Something] nvarchar(4000) NOT NULL DEFAULT \'\',
            [Other] nvarchar(4000) NOT NULL DEFAULT \'\',
            [_timestamp] datetime2,
            PRIMARY KEY NONCLUSTERED("VisitID","Value","MenuItem") NOT ENFORCED
            );',
                    $this->getDestinationSchemaName()
                ));
                break;
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSynapseConnection();
        $this->qb = new SqlBuilder();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    private function getSynapseConnection(): Connection
    {
        return DriverManager::getConnection([
            'user' => (string) getenv('SYNAPSE_UID'),
            'password' => (string) getenv('SYNAPSE_PWD'),
            'host' => (string) getenv('SYNAPSE_SERVER'),
            'dbname' => (string) getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
            'driver' => 'pdo_sqlsrv',
            'driverOptions'=>[
                'ConnectRetryCount' => 5,
                'ConnectRetryInterval' => 10,
//                \PDO::SQLSRV_ATTR_QUERY_TIMEOUT => 1
            ],
        ]);
    }

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     */
    protected function assertTableEqualsExpected(
        SourceInterface $source,
        Table $table,
        SynapseImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new SynapseTableReflection(
            $this->connection,
            $table->getSchema(),
            $table->getTableName()
        ))->getColumnsNames();

        if ($options->useTimestamp()) {
            $this->assertContains('_timestamp', $tableColumns);
        } else {
            $this->assertNotContains('_timestamp', $tableColumns);
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
            'SELECT %s FROM %s',
            implode(', ', $tableColumns),
            $table->getQuotedTableWithScheme()
        );

        $queryResult = array_map(function ($row) {
            return array_map(function ($column) {
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

    /**
     * @param null|SynapseImportOptions::TABLE_TYPES_* $castValueTypes
     * @param null|SynapseImportOptions::SAME_TABLES_* $requireSameTables
     * @param null|SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_* $tableToTableAdapter
     */
    protected function getSynapseImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE,
        ?string $castValueTypes = null,
        ?bool $requireSameTables = null,
        ?string $tableToTableAdapter = null
    ): SynapseImportOptions {
        return new SynapseImportOptions(
            [],
            false,
            true,
            $skipLines,
            // @phpstan-ignore-next-line
            getenv('CREDENTIALS_IMPORT_TYPE'),
            $castValueTypes ?? SynapseImportOptions::TABLE_TYPES_PRESERVE,
            $requireSameTables ?? SynapseImportOptions::SAME_TABLES_NOT_REQUIRED,
            $tableToTableAdapter ?? SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_INSERT_INTO
        );
    }

    protected function getSynapseIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): SynapseImportOptions {
        return new SynapseImportOptions(
            [],
            true,
            true,
            $skipLines,
            // @phpstan-ignore-next-line
            getenv('CREDENTIALS_IMPORT_TYPE')
        );
    }


    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertSynapseTableEqualsExpected(
        SourceInterface $source,
        SynapseTableDefinition $destination,
        SynapseImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new SynapseTableReflection(
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
        }, $this->connection->fetchAllAssociative($sql));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message
        );
    }

    protected function assertSynapseTableExpectedRowCount(
        SynapseTableDefinition $destination,
        SynapseImportOptions $options,
        int $expectedCount,
        string $message = 'Imported tables don\'t have expected number of rows'
    ): void {
        $destRef = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );
        $tableColumns = $destRef->getColumnsNames();

        if ($options->useTimestamp()) {
            self::assertContains('_timestamp', $tableColumns);
        } else {
            self::assertNotContains('_timestamp', $tableColumns);
        }

        self::assertEquals($expectedCount, $destRef->getRowsCount(), $message);
    }
}
