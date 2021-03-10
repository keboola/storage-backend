<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\S3;
use Keboola\Db\ImportExport\Storage\ABS;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;
use Tests\Keboola\Db\ImportExportFunctional\ImportExportBaseTest;

abstract class SnowflakeImportExportBaseTest extends ImportExportBaseTest
{
    protected const SNOWFLAKE_SOURCE_SCHEMA_NAME = 'some.tests';
    protected const SNOWFLAKE_DEST_SCHEMA_NAME = 'in.c-tests';

    protected const STORAGE_S3 = 'S3';
    protected const STORAGE_ABS = 'ABS';

    use ABSSourceTrait;
    use S3SourceTrait;

    /** @var Connection */
    protected $connection;

    /**
     * @param int|string $sortKey
     */
    protected function assertTableEqualsExpected(
        SourceInterface $source,
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

        if (!in_array('_timestamp', $source->getColumnsNames())) {
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
                $this->getSourceSchemaName(),
                $this->getDestinationSchemaName(),
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
        );', $this->getDestinationSchemaName()));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\',
          "_timestamp" TIMESTAMP_NTZ
        );', $this->getDestinationSchemaName()));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."out.csv_2Cols" VALUES
                  (\'x\', \'y\', \'%s\');',
            $this->getDestinationSchemaName(),
            $now
        ));

        $this->connection->query(sprintf('CREATE TABLE "%s"."out.csv_2Cols" (
          "col1" VARCHAR NOT NULL DEFAULT \'\',
          "col2" VARCHAR NOT NULL DEFAULT \'\'
        );', $this->getSourceSchemaName()));

        $this->connection->query(sprintf('INSERT INTO "%s"."out.csv_2Cols" VALUES
                (\'a\', \'b\'), (\'c\', \'d\');
        ', $this->getSourceSchemaName()));

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
            $this->getDestinationSchemaName()
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
            $this->getDestinationSchemaName()
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."table" (
              "column"  varchar(65535) NOT NULL DEFAULT \'\',
              "table" varchar(65535) NOT NULL DEFAULT \'\',
              "_timestamp" TIMESTAMP_NTZ
            );',
            $this->getDestinationSchemaName()
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar NOT NULL,
              "numCol" varchar NOT NULL,
              "floatCol" varchar NOT NULL,
              "boolCol" varchar NOT NULL,
              "_timestamp" TIMESTAMP_NTZ
            );',
            $this->getDestinationSchemaName()
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."types" (
              "charCol"  varchar(65535) NOT NULL,
              "numCol" number(10,1) NOT NULL,
              "floatCol" float NOT NULL,
              "boolCol" boolean NOT NULL
            );',
            $this->getSourceSchemaName()
        ));

        $this->connection->query(sprintf(
            'INSERT INTO "%s"."types" VALUES 
              (\'a\', \'10.5\', \'0.3\', TRUE)
           ;',
            $this->getSourceSchemaName()
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."out.no_timestamp_table" (
              "col1" VARCHAR NOT NULL DEFAULT \'\',
              "col2" VARCHAR NOT NULL DEFAULT \'\'
            );',
            $this->getDestinationSchemaName()
        ));

        $this->connection->query(sprintf(
            'CREATE TABLE "%s"."column-name-row-number" (
              "id" varchar(65535) NOT NULL,
              "row_number" varchar(65535) NOT NULL,
              "_timestamp" TIMESTAMP_NTZ,
              PRIMARY KEY("id")
            );',
            $this->getDestinationSchemaName()
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
            $this->getDestinationSchemaName()
        ));
    }

    private function getSnowflakeConnection(): Connection
    {
        $connection = new Connection([
            'host' => getenv('SNOWFLAKE_HOST'),
            'port' => getenv('SNOWFLAKE_PORT'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'tracing'=>6,
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

    public function getSourceSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return self::SNOWFLAKE_SOURCE_SCHEMA_NAME
            . '-'
            . $buildPrefix;
    }

    public function getDestinationSchemaName(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return self::SNOWFLAKE_DEST_SCHEMA_NAME
            . '-'
            . $buildPrefix;
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     * @return S3\SourceFile|ABS\SourceFile|S3\SourceDirectory|ABS\SourceDirectory
     */
    public function getSourceInstance(
        string $filePath,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ) {
        switch (getenv('STORAGE_TYPE')) {
            case self::STORAGE_S3:
                $getSourceInstance = 'createS3SourceInstance';
                $manifestPrefix = 'S3.';
                break;
            case self::STORAGE_ABS:
                $getSourceInstance = 'createABSSourceInstance';
                $manifestPrefix = '';
                break;
            default:
                throw new \Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }

        $filePath = str_replace('%MANIFEST_PREFIX%', $manifestPrefix, $filePath);
        return $this->$getSourceInstance(
            $filePath,
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys
        );
    }

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeys
     * @return S3\SourceFile|ABS\SourceFile|S3\SourceDirectory|ABS\SourceDirectory
     */
    public function getSourceInstanceFromCsv(
        string $filePath,
        CsvOptions $options,
        array $columns = [],
        bool $isSliced = false,
        bool $isDirectory = false,
        ?array $primaryKeys = null
    ) {
        switch (getenv('STORAGE_TYPE')) {
            case self::STORAGE_S3:
                $getSourceInstanceFromCsv = 'createS3SourceInstanceFromCsv';
                $manifestPrefix = 'S3.';
                break;
            case self::STORAGE_ABS:
                $getSourceInstanceFromCsv = 'createABSSourceInstanceFromCsv';
                $manifestPrefix = '';
                break;
            default:
                throw new \Exception(sprintf('Unknown STORAGE_TYPE "%s".', getenv('STORAGE_TYPE')));
        }

        $filePath = str_replace('%MANIFEST_PREFIX%', $manifestPrefix, $filePath);
        return $this->$getSourceInstanceFromCsv(
            $filePath,
            $options,
            $columns,
            $isSliced,
            $isDirectory,
            $primaryKeys
        );
    }
}
