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
            sprintf("SELECT COUNT(*) FROM %s.%s",
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )));
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
                [
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
                ],
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions(2)
        );

        self::assertEquals(2, $this->connection->fetchOne(
            sprintf("SELECT COUNT(*) FROM %s.%s",
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )));
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
