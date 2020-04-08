<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

abstract class SnowflakeImportExportBaseTest extends ImportExportBaseTest
{
    protected const SNOWFLAKE_SOURCE_SCHEMA_NAME = 'some.tests';
    protected const SNOWFLAKE_DEST_SCHEMA_NAME = 'in.c-tests';

    use ABSSourceTrait;

    /** @var Connection */
    protected $connection;

    /**
     * @param int|string $sortKey
     */
    protected function assertTableEqualsExpected(
        Table $table,
        ImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = $this->connection->getTableColumns($table->getSchema(), $table->getTableName());

        if ($options->useTimestamp()) {
            $this->assertContains('_timestamp', $tableColumns);
        } else {
            $this->assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $table->getColumnsNames())) {
            $tableColumns = array_filter($tableColumns, function ($column) {
                return $column !== '_timestamp';
            });
        }

        // temporary fix of client charset handling
        $columnsSql = array_map(function ($column) {
            return sprintf('BASE64_ENCODE("%s") AS "%s"', $column, $column);
        }, $tableColumns);

        $sql = sprintf(
            'SELECT %s FROM %s',
            implode(', ', $columnsSql),
            $table->getQuotedTableWithScheme()
        );

        $queryResult = array_map(function ($row) {
            return array_map(function ($column) {
                return base64_decode($column);
            }, array_values($row));
        }, $this->connection->fetchAll($sql));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message
        );
    }

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

        $queryResult = $this->connection->fetchAll(
            sprintf(
                'SELECT * FROM %s',
                $this->connection->quoteIdentifier($tableName)
            )
        );

        $this->assertArrayEqualsSorted(
            $filesContent,
            $queryResult,
            $sortKey,
            $message
        );

        if (!empty($filesHeader)) {
            $this->assertSame($filesHeader, array_keys(reset($queryResult)));
        }
    }

    public function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->getSnowflakeConnection();
        $this->initData();
    }

    private function initData(): void
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        $now = $currentDate->format('Y-m-d H:i:s');

        foreach ([
                self::SNOWFLAKE_SOURCE_SCHEMA_NAME,
                self::SNOWFLAKE_DEST_SCHEMA_NAME,
            ] as $schema
        ) {
            $this->connection->query(sprintf('DROP SCHEMA IF EXISTS "%s"', $schema));
            $this->connection->query(sprintf('CREATE SCHEMA "%s"', $schema));
        }

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.lemma" (
          "ts" VARCHAR NOT NULL DEFAULT \'\',
          "lemma" VARCHAR NOT NULL DEFAULT \'\',
          "lemmaIndex" VARCHAR NOT NULL DEFAULT \'\',
          "_timestamp" TIMESTAMP_NTZ
        );', self::SNOWFLAKE_DEST_SCHEMA_NAME));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\',
          "_timestamp" TIMESTAMP_NTZ
        );', self::SNOWFLAKE_DEST_SCHEMA_NAME));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."out.csv_2Cols" VALUES
                  (\'x\', \'y\', \'%s\');',
            self::SNOWFLAKE_DEST_SCHEMA_NAME,
            $now
        ));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\'
        );', self::SNOWFLAKE_SOURCE_SCHEMA_NAME));

        $this->connection->query(sprintf('INSERT INTO "%s"."out.csv_2Cols" VALUES
                (\'a\', \'b\'), (\'c\', \'d\');
        ', self::SNOWFLAKE_SOURCE_SCHEMA_NAME));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."accounts-3" (
                "id" varchar(65535) NOT NULL,
                "idTwitter" varchar(65535) NOT NULL,
                "name" varchar(65535) NOT NULL,
                "import" varchar(65535) NOT NULL,
                "isImported" varchar(65535) NOT NULL,
                "apiLimitExceededDatetime" varchar(65535) NOT NULL,
                "analyzeSentiment" varchar(65535) NOT NULL,
                "importKloutScore" varchar(65535) NOT NULL,
                "timestamp" varchar(65535) NOT NULL,
                "oauthToken" varchar(65535) NOT NULL,
                "oauthSecret" varchar(65535) NOT NULL,
                "idApp" varchar(65535) NOT NULL,
                "_timestamp" TIMESTAMP_NTZ,
                PRIMARY KEY("id")
            )',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."accounts-bez-ts" (
                "id" varchar(65535) NOT NULL,
                "idTwitter" varchar(65535) NOT NULL,
                "name" varchar(65535) NOT NULL,
                "import" varchar(65535) NOT NULL,
                "isImported" varchar(65535) NOT NULL,
                "apiLimitExceededDatetime" varchar(65535) NOT NULL,
                "analyzeSentiment" varchar(65535) NOT NULL,
                "importKloutScore" varchar(65535) NOT NULL,
                "timestamp" varchar(65535) NOT NULL,
                "oauthToken" varchar(65535) NOT NULL,
                "oauthSecret" varchar(65535) NOT NULL,
                "idApp" varchar(65535) NOT NULL,
                PRIMARY KEY("id")
            )',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."table" (
              "column"  varchar(65535) NOT NULL DEFAULT \'\',
              "table" varchar(65535) NOT NULL DEFAULT \'\',
              "_timestamp" TIMESTAMP_NTZ
            );',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar NOT NULL,
              "numCol" varchar NOT NULL,
              "floatCol" varchar NOT NULL,
              "boolCol" varchar NOT NULL,
              "_timestamp" TIMESTAMP_NTZ
            );',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar(65535) NOT NULL,
              "numCol" number(10,1) NOT NULL,
              "floatCol" float NOT NULL,
              "boolCol" boolean NOT NULL
            );',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."types" VALUES 
              (\'a\', \'10.5\', \'0.3\', TRUE)
           ;',
            self::SNOWFLAKE_SOURCE_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."out.no_timestamp_table" (
              "col1" VARCHAR NOT NULL DEFAULT \'\',
              "col2" VARCHAR NOT NULL DEFAULT \'\'
            );',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."column-name-row-number" (
              "id" varchar(65535) NOT NULL,
              "row_number" varchar(65535) NOT NULL,
              "_timestamp" TIMESTAMP_NTZ,
              PRIMARY KEY("id")
            );',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."multi-pk" (
            "VisitID" VARCHAR NOT NULL DEFAULT \'\',
            "Value" VARCHAR NOT NULL DEFAULT \'\',
            "MenuItem" VARCHAR NOT NULL DEFAULT \'\',
            "Something" VARCHAR NOT NULL DEFAULT \'\',
            "Other" VARCHAR NOT NULL DEFAULT \'\',
            "_timestamp" TIMESTAMP_NTZ,
            PRIMARY KEY("VisitID","Value","MenuItem")
            );',
            self::SNOWFLAKE_DEST_SCHEMA_NAME
        ));
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

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->connection);
    }
}
