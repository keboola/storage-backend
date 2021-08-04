<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
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

    public function testLongColumnImport6k(): void
    {
        $this->initTable(self::TABLE_OUT_CSV_2COLS);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        // TODO columns have to match stg table columns, but we want to add _timestamp
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
                'long_col_6k.csv',
                new CsvOptions(),
                [
                    'col1',
                    'col2',
                ],
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        self::assertEquals(2, $this->connection->fetchOne(
            sprintf(
                'SELECT COUNT(*) FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        ));
    }

    public function testWithSkippingLines(): void
    {
        // file has 4 lines in total (including header which is considered as data).
        // Setting skip lines = 2 -> 2 lines should be imported
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
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                'tw_accounts.csv',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions(2)
        );

        $importedData = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        self::assertCount(2, $importedData);
        self::assertCount(12, $importedData[0]);
    }

    public function testWithDirectory(): void
    {
        // file has 4 lines in total (including header which is considered as data).
        // Setting skip lines = 2 -> 2 lines should be imported
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
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                'sliced_accounts_no_manifest',
                new CsvOptions(),
                self::TWITTER_COLUMNS,
                true,
                true,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        $importedData = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        self::assertCount(3, $importedData);
        self::assertCount(12, $importedData[0]);
    }

    public function testWithTabsAsSeparators(): void
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
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv(
                'tw_accounts.tabs.csv',
                new CsvOptions(chr(9), ''),
                self::TWITTER_COLUMNS,
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        $importedData = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        self::assertCount(4, $importedData);
        self::assertCount(12, $importedData[0]);
    }

    public function testWithExtraEnclosures(): void
    {
        $this->initTable(self::TABLE_OUT_CSV_2COLS);

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
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
                'escaping/standard-with-enclosures.csv',
                new CsvOptions(',', '"'),
                [
                    'col1',
                    'col2',
                ],
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        self::assertEquals(8, $this->connection->fetchOne(
            sprintf(
                'SELECT COUNT(*) FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        ));
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
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        self::assertEquals([
            ['id' => 'id', 'col1' => 'name', 'col2' => 'price'],
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

    protected function getExasolImportOptions(
        int $skipLines = 0
    ): ExasolImportOptions {
        return new ExasolImportOptions(
            [],
            false,
            false,
            $skipLines
        );
    }
}
