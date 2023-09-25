<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class StageToFinalCastingErrorsTest extends SnowflakeBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    public function castingErrorCases(): Generator
    {
        yield 'BINARY string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_BINARY)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/The following string is not a legal hex-encoded value/',
        ];
        yield 'VARBINARY string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_VARBINARY)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/The following string is not a legal hex-encoded value/',
        ];
        yield 'OBJECT string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_OBJECT)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/Failed to cast value .* to OBJECT/',
        ];
        yield 'GEOGRAPHY string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_GEOGRAPHY)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/Error parsing Geo input/',
        ];
        yield 'GEOMETRY string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_GEOMETRY)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/Error parsing Geo input/',
        ];
        yield 'TIMESTAMP string' => [
            'column' => new SnowflakeColumn('id', new Snowflake(Snowflake::TYPE_TIMESTAMP)),
            'insertData' => '\'xxx\'',
            'expectedMessage' => '/is not recognized/',
        ];
    }

    /**
     * @dataProvider castingErrorCases
     */
    public function testLoadTypedTableWithCastingValuesErrors(
        SnowflakeColumn $column,
        string $insertData,
        string $expectedMessage,
    ): void {
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'CREATE TABLE %s."types" (
              "%s" %s,
              "_timestamp" TIMESTAMP
            );',
            SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
            $column->getColumnName(),
            $column->getColumnDefinition()->getSQLDefinition(),
        ));

        // skipping header
        $options = new SnowflakeImportOptions(
            [],
            false,
            false,
            1,
            SnowflakeImportOptions::SAME_TABLES_NOT_REQUIRED,
            SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME]
        );

        $destinationRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'types'
        );
        /** @var SnowflakeTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createVarcharStagingTableDefinition(
            $destination->getSchemaName(),
            [
                $column->getColumnName(),
            ]
        );

        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("%s") SELECT %s;',
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName(),
            $column->getColumnName(),
            $insertData
        ));
        $toFinalTableImporter = new FullImporter($this->connection);

        try {
            $toFinalTableImporter->importToTable(
                $stagingTable,
                $destination,
                $options,
                new ImportState($stagingTable->getTableName())
            );
            $this->fail('Import must fail');
        } catch (Exception $e) {
            $this->assertMatchesRegularExpression($expectedMessage, $e->getMessage());
        }
    }
}
