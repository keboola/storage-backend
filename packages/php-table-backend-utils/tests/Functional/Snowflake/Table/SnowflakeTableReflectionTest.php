<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Table;

use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableType;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;

/**
 * @covers SnowflakeTableReflection
 * @uses   ColumnCollection
 */
class SnowflakeTableReflectionTest extends SnowflakeBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testGetTableColumnsNames(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());

        // same test on view
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);

        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
    }

    public function testGetTableColumnsNamesCase(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());

        // same test on view
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertSame([
            'id',
            'first_name',
            'last_name',
        ], $ref->getColumnsNames());
    }

    public function testGetPrimaryKeysNames(): void
    {
        $this->initTable();
        $this->connection->executeQuery(
            sprintf(
                'ALTER TABLE %s.%s ADD PRIMARY KEY ("id")',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(['id'], $ref->getPrimaryKeysNames());

        // same test on view, view has no primary keys
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertEquals([], $ref->getPrimaryKeysNames());
    }

    public function testGetRowsCount(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertEquals(0, $ref->getRowsCount());
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, ...$item);
        }
        self::assertEquals(2, $ref->getRowsCount());

        // same test on view
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        // view doesn't have rows count
        self::assertEquals(0, $ref->getRowsCount());
    }

    public function testGetTableStatsWithWrongCase(): void
    {
        // check upper case because SHOW TABLES is case-insensitive
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, strtoupper(self::TABLE_GENERIC));
        $this->expectException(TableNotExistsReflectionException::class);
        $ref->getTableStats();
    }

    public function testGetTableStats(): void
    {
        $this->initTable();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        $stats1 = $ref->getTableStats();
        self::assertEquals(0, $stats1->getRowsCount());
        self::assertEquals(0, $stats1->getDataSizeBytes()); // empty tables take up some space

        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 1, 'lojza', 'lopata');
        $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, 2, 'karel', 'motycka');

        /** @var TableStats $stats2 */
        $stats2 = $ref->getTableStats();
        self::assertEquals(2, $stats2->getRowsCount());
        self::assertGreaterThan($stats1->getDataSizeBytes(), $stats2->getDataSizeBytes());

        // same test on view
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        /** @var TableStats $stats2 */
        $stats2 = $ref->getTableStats();
        // view doesn't have size or row count
        self::assertEquals(0, $stats2->getRowsCount());
        self::assertEquals(0, $stats2->getDataSizeBytes());
    }

    /**
     * @dataProvider tableColsDataProvider
     */
    public function testColumnDefinition(
        string $sqlDef,
        string $expectedSqlDefinition,
        string $expectedType,
        ?string $expectedDefault,
        ?string $expectedLength,
        bool $expectedNullable
    ): void {
        $this->cleanSchema(self::TEST_SCHEMA);
        $this->createSchema(self::TEST_SCHEMA);
        $sql = sprintf(
            '
            CREATE OR REPLACE TABLE %s.%s (
      "firstColumn" INT,
      "column" %s
);',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            $sqlDef
        );

        $this->connection->executeQuery($sql);
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        /** @var Generator<SnowflakeColumn> $iterator */
        $iterator = $ref->getColumnsDefinitions()->getIterator();
        $iterator->next();
        $column = $iterator->current();
        /** @var Snowflake $definition */
        $definition = $column->getColumnDefinition();
        self::assertEquals($expectedLength, $definition->getLength(), 'length doesnt match');
        self::assertEquals($expectedDefault, $definition->getDefault(), 'default value doesnt match');
        self::assertEquals($expectedType, $definition->getType(), 'type doesnt match');
        self::assertEquals($expectedNullable, $definition->isNullable(), 'nullable flag doesnt match');
        self::assertEquals($expectedSqlDefinition, $definition->getSQLDefinition(), 'SQL definition doesnt match');
    }

    /**
     * @return Generator<string,array<mixed>>
     */
    public function tableColsDataProvider(): Generator
    {
        yield 'DECIMAL' => [
            'DECIMAL', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'NUMERIC' => [
            'NUMERIC', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'NUMERIC 20' => [
            'NUMERIC (20,0)', // sql which goes to table
            'NUMBER (20,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '20,0', // length
            true, // nullable
        ];
        yield 'INT' => [
            'INT', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'INTEGER' => [
            'INTEGER', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'BIGINT' => [
            'BIGINT', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'SMALLINT' => [
            'SMALLINT', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'TINYINT' => [
            'TINYINT', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'BYTEINT' => [
            'BYTEINT', // sql which goes to table
            'NUMBER (38,0)', // expected sql from getSQLDefinition
            'NUMBER', // expected type from db
            null, // default
            '38,0', // length
            true, // nullable
        ];
        yield 'FLOAT' => [
            'FLOAT', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'FLOAT4' => [
            'FLOAT4', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'FLOAT8' => [
            'FLOAT8', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DOUBLE' => [
            'DOUBLE', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DOUBLE PRECISION' => [
            'DOUBLE PRECISION', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'REAL' => [
            'REAL', // sql which goes to table
            'FLOAT', // expected sql from getSQLDefinition
            'FLOAT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'VARCHAR' => [
            'VARCHAR', // sql which goes to table
            'VARCHAR (16777216)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '16777216', // length
            true, // nullable
        ];
        yield 'CHAR' => [
            'CHAR', // sql which goes to table
            'VARCHAR (1)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];
        yield 'CHARACTER' => [
            'CHARACTER', // sql which goes to table
            'VARCHAR (1)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '1', // length
            true, // nullable
        ];
        yield 'STRING' => [
            'STRING', // sql which goes to table
            'VARCHAR (16777216)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '16777216', // length
            true, // nullable
        ];
        yield 'TEXT' => [
            'TEXT', // sql which goes to table
            'VARCHAR (16777216)', // expected sql from getSQLDefinition
            'VARCHAR', // expected type from db
            null, // default
            '16777216', // length
            true, // nullable
        ];
        yield 'BOOLEAN' => [
            'BOOLEAN', // sql which goes to table
            'BOOLEAN', // expected sql from getSQLDefinition
            'BOOLEAN', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATE' => [
            'DATE', // sql which goes to table
            'DATE', // expected sql from getSQLDefinition
            'DATE', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'DATETIME' => [
            'DATETIME', // sql which goes to table
            'TIMESTAMP_NTZ (9)', // expected sql from getSQLDefinition
            'TIMESTAMP_NTZ', // expected type from db
            null, // default
            '9', // length
            true, // nullable
        ];
        yield 'TIME' => [
            'TIME', // sql which goes to table
            'TIME (9)', // expected sql from getSQLDefinition
            'TIME', // expected type from db
            null, // default
            '9', // length
            true, // nullable
        ];
        yield 'TIMESTAMP' => [
            'TIMESTAMP', // sql which goes to table
            'TIMESTAMP_NTZ (9)', // expected sql from getSQLDefinition
            'TIMESTAMP_NTZ', // expected type from db
            null, // default
            '9', // length
            true, // nullable
        ];
        yield 'VARIANT' => [
            'VARIANT', // sql which goes to table
            'VARIANT', // expected sql from getSQLDefinition
            'VARIANT', // expected type from db
            null, // default
            null, // length
            true, // nullable
        ];
        yield 'BINARY' => [
            'BINARY', // sql which goes to table
            'BINARY (8388608)', // expected sql from getSQLDefinition
            'BINARY', // expected type from db
            null, // default
            '8388608', // length
            true, // nullable
        ];
        yield 'VARBINARY' => [
            'VARBINARY', // sql which goes to table
            'BINARY (8388608)', // expected sql from getSQLDefinition
            'BINARY', // expected type from db
            null, // default
            '8388608', // length
            true, // nullable
        ];
    }

    public function testGetDependentViews(): void
    {
        $this->initTable();
        // create extra table and view to check that it finds the one which we are looking for
        $this->initTable(self::TEST_SCHEMA, 'newTable', false);

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);

        self::assertCount(0, $ref->getDependentViews());

        $this->initView();
        $this->initView('newView', 'newTable');

        $dependentViews = $ref->getDependentViews();
        self::assertCount(1, $dependentViews);

        self::assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => self::VIEW_GENERIC,
        ], $dependentViews[0]);
    }

    public function testDependenciesWithCaseSensitivity(): void
    {
        $this->cleanSchema('TEST_UTIL_SCHEMA');
        $this->initTable('TEST_UTIL_SCHEMA', 'case_sensitive'); // A
        $this->initTable('TEST_UTIL_SCHEMA', 'CASE_SENSITIVE', false); // B

        $refA = new SnowflakeTableReflection($this->connection, 'TEST_UTIL_SCHEMA', 'case_sensitive');
        $refB = new SnowflakeTableReflection($this->connection, 'TEST_UTIL_SCHEMA', 'CASE_SENSITIVE');

        self::assertCount(0, $refA->getDependentViews());
        self::assertCount(0, $refB->getDependentViews());

        $this->connection->executeQuery(
            'CREATE VIEW TEST_UTIL_SCHEMA.A_ESCAPED AS SELECT * FROM TEST_UTIL_SCHEMA."case_sensitive";'
        );
        $this->connection->executeQuery(
            'CREATE VIEW TEST_UTIL_SCHEMA.B_UPPER AS SELECT * FROM TEST_UTIL_SCHEMA.CASE_SENSITIVE;'
        );
        $this->connection->executeQuery(
            'CREATE VIEW TEST_UTIL_SCHEMA.B_UPPER_ESCAPED AS SELECT * FROM TEST_UTIL_SCHEMA."CASE_SENSITIVE";'
        );
        $this->connection->executeQuery(
            'CREATE VIEW TEST_UTIL_SCHEMA.B_UPPER_AUTO AS SELECT * FROM TEST_UTIL_SCHEMA.case_sensitive;'
        );

        $dependentViewsA = $refA->getDependentViews();
        self::assertCount(1, $dependentViewsA);

        $dependentViewsB = $refB->getDependentViews();
        self::assertCount(3, $dependentViewsB);

        self::assertSame(
            [
                [
                    'schema_name' => 'TEST_UTIL_SCHEMA',
                    'name' => 'A_ESCAPED',
                ],
            ],
            $dependentViewsA
        );

        self::assertSame(
            [
                [
                    'schema_name' => 'TEST_UTIL_SCHEMA',
                    'name' => 'B_UPPER',
                ],
                [
                    'schema_name' => 'TEST_UTIL_SCHEMA',
                    'name' => 'B_UPPER_AUTO',
                ],
                [
                    'schema_name' => 'TEST_UTIL_SCHEMA',
                    'name' => 'B_UPPER_ESCAPED',
                ],
            ],
            $dependentViewsB
        );
    }

    public function testGetDependentViewsInAnotherSchema(): void
    {
        $this->cleanSchema('anotherSchema');
        $this->createSchema(self::TEST_SCHEMA);
        // create extra table and view to check that it finds the one which we are looking for
        $this->initTable('anotherSchema', 'tableInAnotherSchema');

        $ref = new SnowflakeTableReflection($this->connection, 'anotherSchema', 'tableInAnotherSchema');

        self::assertCount(0, $ref->getDependentViews());

        $this->initView(self::VIEW_GENERIC, 'tableInAnotherSchema', self::TEST_SCHEMA, 'anotherSchema');

        $dependentViews = $ref->getDependentViews();
        self::assertCount(1, $dependentViews);

        self::assertSame([
            'schema_name' => self::TEST_SCHEMA,
            'name' => self::VIEW_GENERIC,
        ], $dependentViews[0]);
    }

    public function testDetectTempTable(): void
    {
        // init table first because of init of schema
        $this->initTable();

        // check temp table on normal table
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertFalse($ref->isTemporary());
        self::assertFalse($ref->isView());

        // check temp table on TEMP table
        $tableName = 'tableWhichDoesntLookLikeTemp';
        $this->connection->executeQuery(
            sprintf(
                'CREATE TEMPORARY TABLE %s.%s (id INT)',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier($tableName)
            )
        );
        $refTemp = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, $tableName);
        self::assertTrue($refTemp->isTemporary());

        $this->initView(self::VIEW_GENERIC, $tableName);
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertFalse($ref->isTemporary());
        self::assertTrue($ref->isView());
    }

    public function testDetectTempTableWithWrongCase(): void
    {
        // create table. it is escaped so lower case
        $this->initTable();

        // check upper case because SHOW TABLES is case-insensitive
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, strtoupper(self::TABLE_GENERIC));
        $this->expectException(TableNotExistsReflectionException::class);
        self::assertFalse($ref->isTemporary());
    }

    private function initView(
        string $viewName = self::VIEW_GENERIC,
        string $tableName = self::TABLE_GENERIC,
        string $viewSchema = self::TEST_SCHEMA,
        string $tableSchema = self::TEST_SCHEMA
    ): void {
        $this->connection->executeQuery(
            sprintf(
                'CREATE VIEW %s.%s AS SELECT * FROM %s.%s;',
                SnowflakeQuote::quoteSingleIdentifier($viewSchema),
                SnowflakeQuote::quoteSingleIdentifier($viewName),
                SnowflakeQuote::quoteSingleIdentifier($tableSchema),
                SnowflakeQuote::quoteSingleIdentifier($tableName)
            )
        );
    }

    public function testIfTableExists(): void
    {
        $this->initTable();

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TABLE_GENERIC);
        self::assertTrue($ref->exists());
        self::assertFalse($ref->isView());

        // same test on view
        $this->initView();
        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::VIEW_GENERIC);
        self::assertTrue($ref->exists());
        self::assertTrue($ref->isView());
    }

    public function testIfSchemaDoesNotExists(): void
    {
        $ref = new SnowflakeTableReflection($this->connection, 'noSchema', 'notExisting');
        self::assertFalse($ref->exists());
    }

    public function testIfTableDoesNotExists(): void
    {
        $this->initTable();

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, 'notExisting');
        self::assertFalse($ref->exists());
    }

    public function testDetectVirtualColumn(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
        $this->connection->executeQuery(
            <<<SQL
CREATE OR REPLACE TABLE CAR_SALES
    (
     SRC variant,
     DEALER VARCHAR(255) AS (src:dealership::string)
)
AS
SELECT PARSE_JSON(column1) AS src
FROM VALUES
         ('{"date":"2017-04-28","dealership":"Valley View Auto Sales"}'),
         ('{"date":"2017-04-28","dealership":"Tindel Toyota"}') v;
SQL
        );

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, 'CAR_SALES');

        $columns = $ref->getColumnsDefinitions();
        $this->assertEquals(['SRC', 'DEALER'], $ref->getColumnsNames());
        $expectedDefinitions = [
            'SRC' => [
                'type' => Snowflake::TYPE_VARIANT,
                'length' => null,
                'nullable' => true,
                ],
            'DEALER' => [
                'type' => Snowflake::TYPE_VARCHAR,
                'length' => '255',
                'nullable' => true,
            ],
        ];
        foreach ($columns as $column) {
            $this->assertSame(
                $expectedDefinitions[$column->getColumnName()],
                $column->getColumnDefinition()->toArray()
            );
        }

        $data = $this->connection->fetchAllAssociative('SELECT * FROM car_sales');
        $this->assertSame([
            [
                'SRC' => '{
  "date": "2017-04-28",
  "dealership": "Valley View Auto Sales"
}',
                'DEALER' => 'Valley View Auto Sales',
            ],
            [
                'SRC' => '{
  "date": "2017-04-28",
  "dealership": "Tindel Toyota"
}',
                'DEALER' => 'Tindel Toyota',
            ],
        ], $data);
        /** @var SnowflakeTableDefinition $definition */
        $definition = $ref->getTableDefinition();
        $this->assertEquals(TableType::TABLE, $definition->getTableType());
    }

    public function testDetectExternalTable(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
        $this->connection->executeQuery(
            <<<SQL
CREATE OR REPLACE STAGE s3_stage URL = 's3://xxxx'
    CREDENTIALS = ( AWS_KEY_ID = 'XXX' AWS_SECRET_KEY = 'YYY');
SQL
        );
        $this->connection->executeQuery(
            <<<SQL
CREATE OR REPLACE
EXTERNAL TABLE MY_LITTLE_EXT_TABLE (
    ID NUMBER(38,0) AS (VALUE:c1::INT),
    FIRST_NAME VARCHAR(255) AS (VALUE:c2::STRING)
    ) 
    LOCATION=@s3_stage/data 
    REFRESH_ON_CREATE = FALSE 
    AUTO_REFRESH = FALSE 
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 TRIM_SPACE=TRUE );
SQL
        );

        // external table is considered as a table just with external flag
        $refSchema = new SnowflakeSchemaReflection($this->connection, self::TEST_SCHEMA);
        $this->assertEquals(['MY_LITTLE_EXT_TABLE'], $refSchema->getTablesNames());

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, 'MY_LITTLE_EXT_TABLE');
        /** @var SnowflakeTableDefinition $definition */
        $definition = $ref->getTableDefinition();
        $this->assertEquals(TableType::SNOWFLAKE_EXTERNAL, $definition->getTableType());
        // value is an implicit column for external tables
        $this->assertEquals(['VALUE', 'ID', 'FIRST_NAME'], $ref->getColumnsNames());
    }
}
