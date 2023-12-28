<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToStage;

use Doctrine\DBAL\Exception;
use Generator;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception as LegacyImportException;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportCommon\StorageType;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class StageImportFileTest extends SnowflakeBaseTestCase
{
    private const TWITTER_COLUMNS = [
        'id',
        'idTwitter',
        'name',
        'import',
        'isImported',
        'apiLimitExceededDatetime',
        'analyzeSentiment',
        'importKloutScore',
        'timestamp',
        'oauthToken',
        'oauthSecret',
        'idApp',
    ];

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->createSchema($this->getDestinationSchemaName());

        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
    }

    /**
     * @dataProvider importSettingProvider
     * @param array{string, CsvOptions, array<string>, bool, bool} $sourceSetting
     * @throws Exception
     */
    public function testImport(
        string $table,
        array $sourceSetting,
        int $expectedNumberOfRows,
        int $expectedFirstLineLength,
        int $skippedLines = 0,
        bool $markAsSkipped = false,
    ): void {
        if ($markAsSkipped) {
            $this->markTestSkipped('Skipping test SourceDirectory not implemented.');
        }
        $this->initTable($table);

        $importer = new ToStageImporter($this->connection);
        $ref = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            $table,
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames(),
        );
        $qb = new SnowflakeTableQueryBuilder();

        $this->connection->executeStatement(
            $qb->getCreateTempTableCommand(
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName(),
                $stagingTable->getColumnsDefinitions(),
            ),
        );
        $importer->importToStagingTable(
            $this->getSourceInstanceFromCsv(
                ...$sourceSetting,
            ),
            $stagingTable,
            $this->getSnowflakeImportOptions($skippedLines),
        );

        $importedData = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getTableName()),
            ),
        );
        self::assertCount($expectedNumberOfRows, $importedData);
        self::assertCount($expectedFirstLineLength, $importedData[0]);
    }

    /**
     * @return Generator<string, array{
     *     table:string,
     *     sourceSettings: array<mixed>,
     *     expectedNumberOfRows:int,
     *     expectedFirstLineLength:int,
     *     skippedLines?:int,
     *     markAsSkipped?:bool
     * }>
     */
    public function importSettingProvider(): Generator
    {
        yield 'with enclosures' => [
            'table' => self::TABLE_OUT_CSV_2COLS_WITHOUT_TS,
            'sourceSettings' => [
                'escaping/standard-with-enclosures.csv',
                new CsvOptions(',', '"'),
                [
                    'col1',
                    'col2',
                ],
                false,
                false,
            ],
            'expectedNumberOfRows' => 8,
            'expectedFirstLineLength' => 2,
        ];
        yield 'with tabs as separators' => [
            'table' => self::TABLE_ACCOUNTS_WITHOUT_TS,
            'sourceSettings' => [
                'tw_accounts.tabs.csv',
                // 9 is tabular
                new CsvOptions(chr(9), ''),
                self::TWITTER_COLUMNS,
                false,
                false,
            ],
            'expectedNumberOfRows' => 4,
            'expectedFirstLineLength' => 12,
        ];
        yield 'with manifest' => [
            'table' => self::TABLE_ACCOUNTS_WITHOUT_TS,
            'sourceSettings' => [
                sprintf('sliced/accounts/%s.accounts.csvmanifest', (string) getenv('STORAGE_TYPE')),
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                true,
                false,
            ],
            'expectedNumberOfRows' => 3,
            'expectedFirstLineLength' => 12,
        ];

        $sourceDirClassExists = class_exists(sprintf(
            'Keboola\Db\ImportExport\Storage\%s\SourceDirectory',
            getenv('STORAGE_TYPE'),
        ));
        yield 'with directory' => [
            'table' => self::TABLE_ACCOUNTS_WITHOUT_TS,
            'sourceSettings' => [
                'sliced_accounts_no_manifest',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                false,
                true,
            ],
            'expectedNumberOfRows' => 3,
            'expectedFirstLineLength' => 12,
            'skippedLines' => 0,
            'markAsSkipped' => !$sourceDirClassExists,
        ];
        yield 'with single csv' => [
            'table' => self::TABLE_OUT_CSV_2COLS_WITHOUT_TS,
            'sourceSettings' => [
                'long_col_6k.csv',
                new CsvOptions(),
                [
                    'col1',
                    'col2',
                ],
                false,
                false,
            ],
            'expectedNumberOfRows' => 2,
            'expectedFirstLineLength' => 2,
        ];
        // file has 4 lines in total (including header which is considered as data).
        // Setting skip lines = 2 -> 2 lines should be imported
        yield 'with skipped lines' => [
            'table' => self::TABLE_ACCOUNTS_WITHOUT_TS,
            'sourceSettings' => [
                'tw_accounts.csv',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                false,
                false,
            ],
            'expectedNumberOfRows' => 2,
            'expectedFirstLineLength' => 12,
            'skippedLines' => 2,
        ];
    }

    /**
     * @dataProvider nullifyFileProvider
     */
    public function testWithNullifyValue(string $fileName): void
    {
        $this->initTable(self::TABLE_NULLIFY);

        $importer = new ToStageImporter($this->connection);
        $ref = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_NULLIFY,
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames(),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );
        $importer->importToStagingTable(
            $this->getSourceInstanceFromCsv(
                $fileName,
                new CsvOptions(),
                [
                    'id',
                    'col1',
                    'col2',
                ],
                false,
                false,
            ),
            $stagingTable,
            $this->getSnowflakeImportOptions(),
        );

        self::assertSame([
            ['id' => '1', 'col1' => 'test', 'col2' => '50'],
            ['id' => '2', 'col1' => null, 'col2' => '500'],
            ['id' => '3', 'col1' => 'Bageta', 'col2' => null],
        ], $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($stagingTable->getTableName()),
            ),
        ));
    }

    public function nullifyFileProvider(): Generator
    {
        yield 'with empty value' => [
            'nullify.csv',
        ];

        yield 'with empty value in quotes' => [
            'nullify-with-quotes.csv',
        ];
    }

// testCopyIntoInvalidTypes
    public function testInvalidManifestImport(): void
    {
        $this->initTable(self::TABLE_ACCOUNTS_WITHOUT_TS);

        $importer = new ToStageImporter($this->connection);
        $ref = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_ACCOUNTS_WITHOUT_TS,
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames(),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );

        if (getenv('STORAGE_TYPE') === StorageType::STORAGE_ABS) {
            $this->expectException(LegacyImportException::class);
        } else {
            // fails on SQL. Manifest with invalid files is being created during loadS3
            $this->expectException(FileNotFoundException::class);
        }

        $importer->importToStagingTable(
            $this->getSourceInstanceFromCsv(
                '02_tw_accounts.csv.invalid.manifest',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                true,
                false,
            ),
            $stagingTable,
            $this->getSnowflakeImportOptions(),
        );
    }

    public function testCopyIntoInvalidTypes(): void
    {
        $this->initTable(self::TABLE_TYPES);

        $importer = new ToStageImporter($this->connection);
        $ref = new SnowflakeTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TYPES,
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames(),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable),
        );

        $this->expectException(LegacyImportException::class);

        $importer->importToStagingTable(
            $this->getSourceInstanceFromCsv(
                'typed_table.invalid-types.csv',
                new CsvOptions(),
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ],
                false,
                false,
            ),
            $stagingTable,
            $this->getSnowflakeImportOptions(),
        );
    }
}
