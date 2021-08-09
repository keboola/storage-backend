<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Doctrine\DBAL\Exception;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;

class StageImportS3Test extends ExasolBaseTestCase
{
    use S3SourceTrait;

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
     * @dataProvider s3ImportSettingProvider
     * @param string $table
     * @param array{string, CsvOptions, array, bool, bool} $s3Setting
     * @param int $expectedRowsNumber
     * @param int $expectedFirstLine
     * @param int $skippedLines
     * @throws Exception
     */
    public function testImportS3(
        string $table,
        array $s3Setting,
        int $expectedRowsNumber,
        int $expectedFirstLine,
        int $skippedLines = 0
    ): void {
        $this->initTable($table);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            $table
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                ...$s3Setting
            ),
            $stagingTable,
            $this->getExasolImportOptions($skippedLines)
        );

        $importedData = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        self::assertCount($expectedRowsNumber, $importedData);
        self::assertCount($expectedFirstLine, $importedData[0]);
    }

    /**
     * @return array[]
     */
    public function s3ImportSettingProvider(): array
    {
        return [
            'with enclosures' => [
                'table' => self::TABLE_OUT_CSV_2COLS_WITHOUT_TS,
                's3providerSetting' => [
                    'escaping/standard-with-enclosures.csv',
                    new CsvOptions(',', '"'),
                    [
                        'col1',
                        'col2',
                    ],
                    false,
                    false,
                ],
                'expectedNumberofRows' => 8,
                'expectedFirstLineLength' => 2,
            ],
            'with tabs as separators' => [
                'table' => self::TABLE_ACCOUNTS_BEZ_TS,
                's3providerSetting' => [
                    'tw_accounts.tabs.csv',
                    // 9 is tabular
                    new CsvOptions(chr(9), ''),
                    self::TWITTER_COLUMNS,
                    false,
                    false,
                ],
                'expectedNumberofRows' => 4,
                'expectedFirstLineLength' => 12,
            ],
            'with manifest' => [
                'table' => self::TABLE_ACCOUNTS_BEZ_TS,
                's3providerSetting' => [
                    'sliced/accounts/S3.accounts.csvmanifest',
                    new CsvOptions(),
                    self::TWITTER_COLUMNS,
                    true,
                    false,
                ],
                'expectedNumberofRows' => 3,
                'expectedFirstLineLength' => 12,
            ],
            'with directory' => [
                'table' => self::TABLE_ACCOUNTS_BEZ_TS,
                's3providerSetting' => [
                    'sliced_accounts_no_manifest',
                    new CsvOptions(),
                    self::TWITTER_COLUMNS,
                    false,
                    true,
                ],
                'expectedNumberofRows' => 3,
                'expectedFirstLineLength' => 12,
            ],
            'with single csv' => [
                'table' => self::TABLE_OUT_CSV_2COLS_WITHOUT_TS,
                's3providerSetting' => [
                    'long_col_6k.csv',
                    new CsvOptions(),
                    [
                        'col1',
                        'col2',
                    ],
                    false,
                    false,
                ],
                'expectedNumberofRows' => 2,
                'expectedFirstLineLength' => 2,
            ],
            // file has 4 lines in total (including header which is considered as data).
            // Setting skip lines = 2 -> 2 lines should be imported
            'with skipped lines' => [
                'table' => self::TABLE_ACCOUNTS_BEZ_TS,
                's3providerSetting' => [
                    'tw_accounts.csv',
                    new CsvOptions(),
                    self::TWITTER_COLUMNS,
                    false,
                    false,
                ],
                'expectedNumberofRows' => 2,
                'expectedFirstLineLength' => 12,
                'skippedLines' => 2,
            ],
        ];
    }

    public function testWithNullifyValue(): void
    {
        $this->initTable(self::TABLE_NULLIFY);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_NULLIFY
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                'nullify.csv',
                new CsvOptions(),
                [
                    'id',
                    'col1',
                    'col2',
                ],
                false,
                false
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        self::assertEquals([
            ['id' => '1', 'col1' => 'test', 'col2' => '50'],
            ['id' => '2', 'col1' => null, 'col2' => '500'],
            ['id' => '3', 'col1' => 'Bageta', 'col2' => null],
        ], $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        ));
    }

// testCopyIntoInvalidTypes
    public function testInvalidManifestImport(): void
    {
        $this->initTable(self::TABLE_ACCOUNTS_BEZ_TS);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_ACCOUNTS_BEZ_TS
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        // fails on SQL, no parsing/checking entries before
        $this->expectException(Exception::class);

        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                '02_tw_accounts.csv.invalid.manifest',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                true,
                false
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );
    }

    public function testCopyIntoInvalidTypes(): void
    {
        $this->initTable(self::TABLE_TYPES);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TYPES
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        $this->expectException(Exception::class);

        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                'typed_table.invalid-types.csv',
                new CsvOptions(),
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ],
                false,
                false
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );
    }
}
