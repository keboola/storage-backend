<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Table;

use Generator;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ReflectionException;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

/**
 * @covers SynapseTableReflection
 * @uses   ColumnCollection
 */
class SynapseTableReflectionTest extends SynapseBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'ref-table-schema';
    // tables
    public const TABLE_GENERIC = self::TESTS_PREFIX . 'ref';
    //views
    public const VIEW_GENERIC = self::TESTS_PREFIX . 'ref-view';

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->createTestSchema();
    }

    protected function createTestSchema(string $schemaName = self::TEST_SCHEMA): void
    {
        $this->connection->exec($this->schemaQb->getCreateSchemaCommand($schemaName));
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $this->assertSame([
            'int_def',
            'var_def',
            'num_def',
            '_time',
        ], $ref->getColumnsNames());
    }

    protected function initTable(
        string $schema = self::TEST_SCHEMA,
        string $table = self::TABLE_GENERIC
    ): void {
        $this->connection->exec(
            sprintf(
                'CREATE TABLE [%s].[%s] (
          [int_def] INT NOT NULL DEFAULT 0,
          [var_def] nvarchar(1000) NOT NULL DEFAULT (\'\'),
          [num_def] NUMERIC(10,5) DEFAULT ((1.00)),
          [_time] datetime2 NOT NULL DEFAULT \'2020-02-01 00:00:00\'
        );',
                $schema,
                $table
            )
        );
    }

    public function testGetTableObjectId(): void
    {
        $this->initTable();
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        $this->assertNotNull($ref->getObjectId());
    }

    public function testGetTableObjectIdWithDots(): void
    {
        $schemaName = self::TEST_SCHEMA . '..dots';
        $table = self::TABLE_GENERIC . '..dots';
        $this->dropAllWithinSchema($schemaName);
        $this->createTestSchema($schemaName);
        $this->initTable($schemaName, $table);

        $ref = new SynapseTableReflection($this->connection, $schemaName, $table);
        $this->assertNotNull($ref->getObjectId());
    }

    public function testGetTempTableObjectIdWithDots(): void
    {
        $schemaName = self::TEST_SCHEMA . '..dots';
        $table = '#'.self::TABLE_GENERIC . '..dots';
        $this->dropAllWithinSchema($schemaName);
        $this->createTestSchema($schemaName);
        $this->initTable($schemaName, $table);

        $ref = new SynapseTableReflection($this->connection, $schemaName, $table);
        $this->assertNotNull($ref->getObjectId());
    }

    public function testGetPrimaryKeysNames(): void
    {
        $this->initTable();
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        $this->assertEmpty($ref->getPrimaryKeysNames());
        $this->connection->exec(sprintf(
            'ALTER TABLE [%s].[%s] ADD CONSTRAINT [PK_1] PRIMARY KEY NONCLUSTERED ([_time], [int_def]) NOT ENFORCED',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC
        ));
        $this->assertSame(['_time', 'int_def'], $ref->getPrimaryKeysNames());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        $this->assertEquals(0, $ref->getRowsCount());
        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[%s] VALUES (10, \'xxx\', 10,\'2020-02-01 00:00:00\')',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC
        ));
        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[%s] VALUES (10, \'xxx\', 10,\'2020-02-01 00:00:00\')',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC
        ));
        $this->assertEquals(2, $ref->getRowsCount());
    }

    /**
     * @return Generator<array{
     *     string,
     *     string,
     *     string,
     *     ?string,
     *     ?string,
     *     bool
     * }>
     */
    public function tableColsDataProvider(): Generator
    {
        yield 'INT NOT NULL DEFAULT 0' => [
            'int NOT NULL DEFAULT ((0))',
            'INT NOT NULL DEFAULT 0',
            'INT',
            '0',
            null,
            false,
        ];
        yield 'INT DEFAULT NULL' => [
            'int NOT NULL DEFAULT ((0))',
            'INT NOT NULL DEFAULT 0',
            'INT',
            '0',
            null,
            false,
        ];
        yield 'nvarchar(1000) NOT NULL DEFAULT (\'\')' => [
            'nvarchar(1000) NOT NULL DEFAULT (\'\')',
            'NVARCHAR(1000) NOT NULL DEFAULT \'\'',
            'NVARCHAR',
            '\'\'',
            '1000',
            false,
        ];
        yield 'NUMERIC(10,5) DEFAULT ((1.00))' => [
            'numeric(10,5) DEFAULT ((1.00))',
            'NUMERIC(10,5) DEFAULT 1.00',
            'NUMERIC',
            '1.00',
            '10,5',
            false,
        ];
        yield 'float' => [
            'float',
            'FLOAT(53)',
            'FLOAT',
            null,
            '53',
            false,
        ];
        yield 'float(1)' => [
            'FLOAT(1)',
            'REAL',
            'REAL',
            null,
            null,
            false,
        ];
        yield 'float(20)' => [
            'float(20)',
            'REAL',
            'REAL',
            null,
            null,
            false,
        ];
        yield 'float(53)' => [
            'float(53)',
            'FLOAT(53)',
            'FLOAT',
            null,
            '53',
            false,
        ];
        yield 'nvarchar' => [
            'nvarchar',
            'NVARCHAR(1)',
            'NVARCHAR',
            null,
            '1',
            false,
        ];
        yield 'nvarchar(1)' => [
            'nvarchar(1)',
            'NVARCHAR(1)',
            'NVARCHAR',
            null,
            '1',
            false,
        ];
        yield 'nvarchar(4000)' => [
            'nvarchar(4000)',
            'NVARCHAR(4000)',
            'NVARCHAR',
            null,
            '4000',
            false,
        ];
        yield 'nvarchar(max)' => [
            'nvarchar(max)',
            'NVARCHAR(MAX)',
            'NVARCHAR',
            null,
            'MAX',
            true,
        ];
        yield 'nchar' => [
            'nchar',
            'NCHAR(1)',
            'NCHAR',
            null,
            '1',
            false,
        ];
        yield 'nchar(1)' => [
            'nchar(1)',
            'NCHAR(1)',
            'NCHAR',
            null,
            '1',
            false,
        ];
        yield 'nchar(4000)' => [
            'nchar(4000)',
            'NCHAR(4000)',
            'NCHAR',
            null,
            '4000',
            false,
        ];
        yield 'varchar' => [
            'varchar',
            'VARCHAR(1)',
            'VARCHAR',
            null,
            '1',
            false,
        ];
        yield 'varchar(1)' => [
            'varchar(1)',
            'VARCHAR(1)',
            'VARCHAR',
            null,
            '1',
            false,
        ];
        yield 'varchar(8000)' => [
            'varchar(8000)',
            'VARCHAR(8000)',
            'VARCHAR',
            null,
            '8000',
            false,
        ];
        yield 'varchar(max)' => [
            'varchar(max)',
            'VARCHAR(MAX)',
            'VARCHAR',
            null,
            'MAX',
            true,
        ];
        yield 'char' => [
            'char',
            'CHAR(1)',
            'CHAR',
            null,
            '1',
            false,
        ];
        yield 'char(1)' => [
            'char(1)',
            'CHAR(1)',
            'CHAR',
            null,
            '1',
            false,
        ];
        yield 'char(4000)' => [
            'char(4000)',
            'CHAR(4000)',
            'CHAR',
            null,
            '4000',
            false,
        ];
        yield 'varbinary' => [
            'varbinary',
            'VARBINARY(1)',
            'VARBINARY',
            null,
            '1',
            false,
        ];
        yield 'varbinary(1)' => [
            'varbinary(1)',
            'VARBINARY(1)',
            'VARBINARY',
            null,
            '1',
            false,
        ];
        yield 'varbinary(8000)' => [
            'varbinary(8000)',
            'VARBINARY(8000)',
            'VARBINARY',
            null,
            '8000',
            false,
        ];
        yield 'varbinary(max)' => [
            'varbinary(max)',
            'VARBINARY(MAX)',
            'VARBINARY',
            null,
            'MAX',
            true,
        ];
        yield 'binary' => [
            'binary',
            'BINARY(1)',
            'BINARY',
            null,
            '1',
            false,
        ];
        yield 'binary(1)' => [
            'binary(1)',
            'BINARY(1)',
            'BINARY',
            null,
            '1',
            false,
        ];
        yield 'binary(8000)' => [
            'binary(8000)',
            'BINARY(8000)',
            'BINARY',
            null,
            '8000',
            false,
        ];
        yield 'datetimeoffset' => [
            'datetimeoffset',
            'DATETIMEOFFSET(7)',
            'DATETIMEOFFSET',
            null,
            '7',
            false,
        ];
        yield 'datetimeoffset(0)' => [
            'datetimeoffset(0)',
            'DATETIMEOFFSET(0)',
            'DATETIMEOFFSET',
            null,
            '0',
            false,
        ];
        yield 'datetimeoffset(7)' => [
            'datetimeoffset(7)',
            'DATETIMEOFFSET(7)',
            'DATETIMEOFFSET',
            null,
            '7',
            false,
        ];
        yield 'datetime2' => [
            'datetime2',
            'DATETIME2(7)',
            'DATETIME2',
            null,
            '7',
            false,
        ];
        yield 'datetime2(0)' => [
            'datetime2(0)',
            'DATETIME2(0)',
            'DATETIME2',
            null,
            '0',
            false,
        ];
        yield 'datetime2(7)' => [
            'datetime2(7)',
            'DATETIME2(7)',
            'DATETIME2',
            null,
            '7',
            false,
        ];
        yield 'time' => [
            'time',
            'TIME(7)',
            'TIME',
            null,
            '7',
            false,
        ];
        yield 'time(0)' => [
            'time(0)',
            'TIME(0)',
            'TIME',
            null,
            '0',
            false,
        ];
        yield 'time(7)' => [
            'time(7)',
            'TIME(7)',
            'TIME',
            null,
            '7',
            false,
        ];
    }

    public function testGetTableColumnsDefinitionsOrder(): void
    {
        $sql = sprintf(
            'CREATE TABLE [%s].[%s] ([col] VARCHAR(50) NOT NULL, [_time] DATE DEFAULT NULL)',
            self::TEST_SCHEMA,
            'table_defs'
        );

        $this->connection->exec($sql);
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, 'table_defs');

        /** @var SynapseColumn[] $definitions */
        $definitions = iterator_to_array($ref->getColumnsDefinitions());
        $this->assertCount(2, $definitions);

        $col1 = $definitions[0];
        $this->assertSame('col', $col1->getColumnName());

        $col2 = $definitions[1];
        $this->assertSame('_time', $col2->getColumnName());
    }

    /**
     * @dataProvider tableColsDataProvider
     */
    public function testGetTableColumnsDefinitions(
        string $sqlDef,
        string $expectedDefinition,
        string $expectedType,
        ?string $expectedDefault,
        ?string $expectedLength,
        bool $useHeapTable = false
    ): void {
        $sql = sprintf(
            'CREATE TABLE [%s].[%s] ([col] %s)',
            self::TEST_SCHEMA,
            'table_defs',
            $sqlDef
        );

        if ($useHeapTable === true) {
            $sql .= ' WITH (HEAP)';
        }

        $this->connection->exec($sql);
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, 'table_defs');
        $definitions = $ref->getColumnsDefinitions();
        $this->assertCount(1, $definitions);

        /** @var SynapseColumn $definition */
        $definition = $definitions->getIterator()->current();

        $this->assertEquals(
            $expectedDefinition,
            $definition->getColumnDefinition()->getSQLDefinition(),
            sprintf('SQL definitions don\'t match real definition was "%s".', $sqlDef)
        );
        $this->assertEquals(
            $expectedType,
            $definition->getColumnDefinition()->getType(),
            'Types don\'t match.'
        );
        $this->assertEquals(
            $expectedDefault,
            $definition->getColumnDefinition()->getDefault(),
            'Defaults don\'t match.'
        );
        $this->assertEquals(
            $expectedLength,
            $definition->getColumnDefinition()->getLength(),
            'Length don\'t match.'
        );
    }

    public function testIsTemporaryTable(): void
    {
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, '#table_defs');
        $this->assertTrue($ref->isTemporary());
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, 'table_defs');
        $this->assertFalse($ref->isTemporary());
    }

    public function testTemporaryTableGetObjectId(): void
    {
        $this->connection->exec($this->tableQb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#table_defs',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, '#table_defs');
        $objectId = $ref->getObjectId();
        $this->assertIsNumeric($objectId);
    }

    public function testTemporaryTableGetRowsCount(): void
    {
        $this->connection->exec($this->tableQb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#table_defs',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, '#table_defs');
        $count = $ref->getRowsCount();
        $this->assertEquals(0, $count);
        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[%s] VALUES (\'xxx\')',
            self::TEST_SCHEMA,
            '#table_defs'
        ));
        $count = $ref->getRowsCount();
        $this->assertEquals(1, $count);
    }

    /**
     * @return Generator<array{string}>
     */
    public function unsupportedOperationsOnTemporaryTableProvider(): \Generator
    {
        yield 'getColumnsNames' => ['getColumnsNames'];
        yield 'getColumnsDefinitions' => ['getColumnsDefinitions'];
        yield 'getPrimaryKeysNames' => ['getPrimaryKeysNames'];
        yield 'getPrimaryKeysNames' => ['getPrimaryKeysNames'];
        yield 'getTableStats' => ['getTableStats'];
    }

    /**
     * @dataProvider unsupportedOperationsOnTemporaryTableProvider
     */
    public function testTemporaryTableGetColumnsNames(string $operation): void
    {
        $this->connection->exec($this->tableQb->getCreateTempTableCommand(
            self::TEST_SCHEMA,
            '#table_defs',
            new ColumnCollection([SynapseColumn::createGenericColumn('col1')])
        ));
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, '#table_defs');
        $this->expectException(ReflectionException::class);
        $ref->{$operation}();
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();

        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $this->assertCount(0, $ref->getDependentViews());

        $this->initView(self::VIEW_GENERIC, self::TABLE_GENERIC);

        $dependentViews = $ref->getDependentViews();
        $this->assertCount(1, $dependentViews);

        $this->assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => self::VIEW_GENERIC,
        ], $dependentViews[0]);
    }

    public function testGetTableStats(): void
    {
        $this->initTable();
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $stats1 = $ref->getTableStats();
        $this->assertEquals(0, $stats1->getRowsCount());
        $this->assertGreaterThan(1024, $stats1->getDataSizeBytes()); // empty tables take up some space

        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[%s] VALUES (10, \'xxx\', 10,\'2020-02-01 00:00:00\')',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC
        ));
        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[%s] VALUES (10, \'xxx\', 10,\'2020-02-01 00:00:00\')',
            self::TEST_SCHEMA,
            self::TABLE_GENERIC
        ));

        $stats2 = $ref->getTableStats();
        $this->assertEquals(2, $stats2->getRowsCount());
        $this->assertGreaterThan($stats1->getDataSizeBytes(), $stats2->getDataSizeBytes());
    }

    /**
     * @return Generator<array{
     *     string,
     *     string,
     * }>
     */
    public function tableDistributionProvider(): \Generator
    {
        yield 'ROUND_ROBIN' => [
            'DISTRIBUTION = ROUND_ROBIN',
            'ROUND_ROBIN',
        ];
        yield 'HASH' => [
            'DISTRIBUTION = HASH (int_def)',
            'HASH',
        ];
        yield 'REPLICATE' => [
            'DISTRIBUTION = REPLICATE',
            'REPLICATE',
        ];
    }

    /**
     * @dataProvider tableDistributionProvider
     */
    public function testGetTableDistribution(string $with, string $expectedDistribution): void
    {
        $this->connection->exec(
            sprintf(
                'CREATE TABLE [%s].[%s] (
          [int_def] INT
        )
        WITH(%s)
        ;',
                self::TEST_SCHEMA,
                self::TABLE_GENERIC,
                $with
            )
        );
        $ref = new SynapseTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals($expectedDistribution, $ref->getTableDistribution());
    }

    private function initView(string $viewName, string $parentName): void
    {
        $this->connection->exec(
            sprintf(
                'CREATE VIEW [%s].[%s] AS SELECT * FROM [%s].[%s];',
                self::TEST_SCHEMA,
                $viewName,
                self::TEST_SCHEMA,
                $parentName
            )
        );
    }
}
