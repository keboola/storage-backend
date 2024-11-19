<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToFinal;

use DateTime;
use Generator;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\DateTimeHelper;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\ToStageImporterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class SqlBuilderTest extends SnowflakeBaseTestCase
{
    public const TESTS_PREFIX = 'import_export_test_';
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'schema';
    public const TEST_SCHEMA_QUOTED = '"' . self::TEST_SCHEMA . '"';
    public const TEST_STAGING_TABLE = '__temp_stagingTable';
    public const TEST_STAGING_TABLE_QUOTED = '"__temp_stagingTable"';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_IN_SCHEMA = self::TEST_SCHEMA_QUOTED . '.' . self::TEST_TABLE_QUOTED;
    public const TEST_TABLE_QUOTED = '"' . self::TEST_TABLE . '"';

    protected function dropTestSchema(): void
    {
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    protected function getBuilder(): SqlBuilder
    {
        return new SqlBuilder();
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropTestSchema();
    }

    protected function createTestSchema(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
    }

    public function testGetDedupCommand(): void
    {
        $this->createTestSchema();
        $stageDef = $this->createStagingTableWithData();

        $deduplicationDef = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            '__temp_tempTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            [
                'pk1',
                'pk2',
            ],
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($deduplicationDef));

        $sql = $this->getBuilder()->getDedupCommand(
            $stageDef,
            $deduplicationDef,
            $deduplicationDef->getPrimaryKeysNames(),
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."__temp_tempTable" ("col1", "col2") SELECT a."col1",a."col2" FROM (SELECT "col1", "col2", ROW_NUMBER() OVER (PARTITION BY "pk1","pk2" ORDER BY "pk1","pk2") AS "_row_number_" FROM "import_export_test_schema"."__temp_stagingTable") AS a WHERE a."_row_number_" = 1',
            $sql,
        );
        $this->connection->executeStatement($sql);
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($deduplicationDef->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($deduplicationDef->getTableName()),
        ));

        self::assertCount(2, $result);
    }

    private function createStagingTableWithData(bool $includeEmptyValues = false): SnowflakeTableDefinition
    {
        $def = $this->getStagingTableDefinition();
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($def));

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'2\',\'2\')',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );

        if ($includeEmptyValues) {
            $this->connection->executeStatement(
                sprintf(
                    'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (2,2,\'\',NULL)',
                    self::TEST_SCHEMA_QUOTED,
                    self::TEST_STAGING_TABLE_QUOTED,
                ),
            );
        }

        return $def;
    }

    private function getDummyImportOptions(): SnowflakeImportOptions
    {
        return new SnowflakeImportOptions([]);
    }

    public function testGetDeleteOldItemsCommand(): void
    {
        $this->createTestSchema();

        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'id',
                    new Snowflake(
                        Snowflake::TYPE_INT,
                    ),
                ),
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $tableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $tableSql,
            ),
        );
        $stagingTableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
            $this->getSimpleImportOptions(),
        );

        self::assertEquals(
        // phpcs:ignore
            'DELETE FROM "import_export_test_schema"."__temp_stagingTable" "src" USING "import_export_test_schema"."import_export_test_test" AS "dest" WHERE "dest"."pk1" = COALESCE("src"."pk1", \'\') AND "dest"."pk2" = COALESCE("src"."pk2", \'\') ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql,
        ));

        self::assertCount(1, $result);
        self::assertSame([
            [
                'pk1' => '2',
                'pk2' => '1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }

    public function testGetDeleteOldItemsCommandRequireSameTables(): void
    {
        $this->createTestSchema();

        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'id',
                    new Snowflake(
                        Snowflake::TYPE_INT,
                    ),
                ),
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $tableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        );
        $qb = new SnowflakeTableQueryBuilder();
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($tableDefinition));
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","pk1","pk2","col1","col2") VALUES (1,1,1,\'1\',\'1\')',
                $tableSql,
            ),
        );
        $stagingTableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('pk1'),
                SnowflakeColumn::createGenericColumn('pk2'),
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            ['pk1', 'pk2'],
        );
        $this->connection->executeStatement($qb->getCreateTableCommandFromDefinition($stagingTableDefinition));
        $stagingTableSql = sprintf(
            '%s.%s',
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($stagingTableDefinition->getTableName()),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (1,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("pk1","pk2","col1","col2") VALUES (2,1,\'1\',\'1\')',
                $stagingTableSql,
            ),
        );

        $sql = $this->getBuilder()->getDeleteOldItemsCommand(
            $stagingTableDefinition,
            $tableDefinition,
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 0,
                requireSameTables: ImportOptions::SAME_TABLES_NOT_REQUIRED,
                nullManipulation: ImportOptions::NULL_MANIPULATION_SKIP, //<- skipp null manipulation,
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
        );

        self::assertEquals(
        // phpcs:ignore
            'DELETE FROM "import_export_test_schema"."__temp_stagingTable" "src" USING "import_export_test_schema"."import_export_test_test" AS "dest" WHERE "dest"."pk1" = "src"."pk1" AND "dest"."pk2" = "src"."pk2" ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            $stagingTableSql,
        ));

        self::assertCount(1, $result);
        self::assertSame([
            [
                'pk1' => '2',
                'pk2' => '1',
                'col1' => '1',
                'col2' => '1',
            ],
        ], $result);
    }

    private function assertTableNotExists(string $schemaName, string $tableName): void
    {
        try {
            (new SnowflakeTableReflection($this->connection, $schemaName, $tableName))->getTableStats();
            self::fail(sprintf(
                'Table "%s.%s" is expected to not exist.',
                $schemaName,
                $tableName,
            ));
        } catch (TableNotExistsReflectionException $e) {
        }
    }

    public function testGetDropTableIfExistsCommand(): void
    {
        $this->createTestSchema();
        self::assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            'DROP TABLE IF EXISTS "import_export_test_schema"."import_export_test_test"',
            $sql,
        );
        $this->connection->executeStatement($sql);

        // create table
        $this->createTestTable();

        // try to drop not existing table
        $sql = $this->getBuilder()->getDropTableIfExistsCommand(self::TEST_SCHEMA, self::TEST_TABLE);
        self::assertEquals(
        // phpcs:ignore
            'DROP TABLE IF EXISTS "import_export_test_schema"."import_export_test_test"',
            $sql,
        );
        $this->connection->executeStatement($sql);

        self::assertTableNotExists(self::TEST_SCHEMA, self::TEST_TABLE);
    }

    protected function createTestTable(): void
    {
        $table = self::TEST_TABLE_IN_SCHEMA;
        $this->connection->executeStatement(<<<EOT
CREATE TABLE $table (
    id int NOT NULL
)
EOT,);
    }

    public function testGetInsertAllIntoTargetTableCommand(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new SnowflakeImportOptions(
                ignoreColumns: ['id'],
            ),
            '2020-01-01 00:00:00',
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT COALESCE("col1", \'\') AS "col1",COALESCE("col2", \'\') AS "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => '',
                'col2' => '',
            ],
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandCasting(): void
    {
        $this->createTestSchema();
        $destination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                new SnowflakeColumn(
                    'VARIANT',
                    new Snowflake(
                        Snowflake::TYPE_VARIANT,
                    ),
                ),
                new SnowflakeColumn(
                    'BINARY',
                    new Snowflake(
                        Snowflake::TYPE_BINARY,
                    ),
                ),
                new SnowflakeColumn(
                    'VARBINARY',
                    new Snowflake(
                        Snowflake::TYPE_VARBINARY,
                    ),
                ),
                new SnowflakeColumn(
                    'OBJECT',
                    new Snowflake(
                        Snowflake::TYPE_OBJECT,
                    ),
                ),
                new SnowflakeColumn(
                    'ARRAY',
                    new Snowflake(
                        Snowflake::TYPE_ARRAY,
                    ),
                ),
                new SnowflakeColumn(
                    'VECTOR',
                    new Snowflake(
                        Snowflake::TYPE_VECTOR,
                        [
                            'length' => 'INT,3',
                        ],
                    ),
                ),
            ]),
            ['pk1'],
        );
        $stage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                $this->createNullableGenericColumn('VARIANT'),
                $this->createNullableGenericColumn('BINARY'),
                $this->createNullableGenericColumn('VARBINARY'),
                $this->createNullableGenericColumn('OBJECT'),
                $this->createNullableGenericColumn('ARRAY'),
                $this->createNullableGenericColumn('VECTOR'),
            ]),
            [],
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($destination),
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($stage),
        );

        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("pk1","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","VECTOR") 
SELECT \'1\', 
       TO_VARIANT(\'4.14\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  24::VARIANT),
       ARRAY_CONSTRUCT(1, 2, 3, NULL),
       ARRAY_CONSTRUCT(1,2,3)::VECTOR(INT,3)
;',
            self::TEST_SCHEMA,
            self::TEST_TABLE,
        ));

        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("pk1","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","VECTOR") 
SELECT \'1\', 
       TO_VARCHAR(TO_VARIANT(\'3.14\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\')),
       TO_VARCHAR(OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT)),
       TO_VARCHAR(ARRAY_CONSTRUCT(1, 2, 3, NULL)),
       TO_VARCHAR(TO_ARRAY([1,2,3]::VECTOR(INT,3)))
;',
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
        ));

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $stage,
            $destination,
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 0,
                requireSameTables: ImportOptions::SAME_TABLES_NOT_REQUIRED,
                nullManipulation: ImportOptions::NULL_MANIPULATION_SKIP, //<- skipp null manipulation
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            '2020-01-01 00:00:00',
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("pk1", "VARIANT", "BINARY", "VARBINARY", "OBJECT", "ARRAY", "VECTOR") (SELECT "pk1",CAST("VARIANT" AS VARIANT) AS "VARIANT","BINARY","VARBINARY",CAST(TO_VARIANT("OBJECT") AS OBJECT) AS "OBJECT",CAST(PARSE_JSON("ARRAY") AS ARRAY) AS "ARRAY",CAST(PARSE_JSON("VECTOR") AS ARRAY)::VECTOR (INT,3) AS "VECTOR" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(1, $out);

        // now Snowflake return `vector = null` when you use `select * from …`, we need to cast vector explicitly here
        // this hack can be removed, when Snowflake start to select vectors correctly
        // Snowflake web console works properly, so this is probably bug in ODBC driver
        // We try driver version 3.4.0 and behaviour was the same
        $result = $this->connection->fetchAllAssociative(sprintf(
        // phpcs:ignore
            'SELECT "pk1", "VARIANT", "BINARY", "VARBINARY", "OBJECT", "ARRAY", cast("VECTOR" AS ARRAY) AS "VECTOR" FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'pk1' => '1',
                'VARIANT' => '"4.14"',
                'BINARY' => '1',
                'VARBINARY' => '1',
                'OBJECT' => <<<EOD
{
  "age": 24,
  "name": "Jones"
}
EOD,
                'ARRAY' => <<<EOD
[
  1,
  2,
  3,
  undefined
]
EOD,
                'VECTOR' => <<<EOD
[
  1,
  2,
  3
]
EOD,
            ],
            [
                'pk1' => '1',
                'VARIANT' => '"3.14"',
                'BINARY' => '1',
                'VARBINARY' => '1',
                'OBJECT' => <<<EOD
{
  "age": 42,
  "name": "Jones"
}
EOD,
                'ARRAY' => <<<EOD
[
  1,
  2,
  3,
  undefined
]
EOD,
                'VECTOR' => <<<EOD
[
  1,
  2,
  3
]
EOD,

            ],
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandSameTables(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);

        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // no convert values no timestamp
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 0,
                requireSameTables: ImportOptions::SAME_TABLES_NOT_REQUIRED,
                nullManipulation: ImportOptions::NULL_MANIPULATION_SKIP, //<- skipp null manipulation
                ignoreColumns: [
                    'id',
                ],
            ),
            '2020-01-01 00:00:00',
        );

        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT "col1","col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );

        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => '',
                'col2' => '',
            ],
        ], $result);
    }

    protected function createTestTableWithColumns(
        bool $includeTimestamp = false,
        bool $includePrimaryKey = false,
    ): SnowflakeTableDefinition {
        $columns = [];
        $pks = [];
        if ($includePrimaryKey) {
            $pks[] = 'id';
            $columns[] = new SnowflakeColumn(
                'id',
                new Snowflake(Snowflake::TYPE_INT),
            );
        } else {
            $columns[] = $this->createNullableGenericColumn('id');
        }
        $columns[] = $this->createNullableGenericColumn('col1');
        $columns[] = $this->createNullableGenericColumn('col2');

        if ($includeTimestamp) {
            $columns[] = new SnowflakeColumn(
                '_timestamp',
                new Snowflake(Snowflake::TYPE_TIMESTAMP),
            );
        }

        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection($columns),
            $pks,
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition),
        );

        return $tableDefinition;
    }

    private function createNullableGenericColumn(string $columnName): SnowflakeColumn
    {
        $definition = new Snowflake(
            Snowflake::TYPE_VARCHAR,
            [
                'length' => '4000', // should be changed to max in future
                'nullable' => true,
            ],
        );

        return new SnowflakeColumn(
            $columnName,
            $definition,
        );
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNull(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // convert col1 to null
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: ['col1'],
            ignoreColumns: ['id'],
        );
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2") (SELECT IFF("col1" = \'\', NULL, "col1"),COALESCE("col2", \'\') AS "col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '1',
                'col2' => '1',
            ],
            [
                'id' => null,
                'col1' => '2',
                'col2' => '2',
            ],
            [
                'id' => null,
                'col1' => null,
                'col2' => '',
            ],
        ], $result);
    }

    public function testGetInsertAllIntoTargetTableCommandConvertToNullWithTimestamp(): void
    {
        $this->createTestSchema();
        $destination = $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        // use timestamp
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: ['col1'],
            isIncremental: false,
            useTimestamp: true,
            ignoreColumns: [
                'id',
                ToStageImporterInterface::TIMESTAMP_COLUMN_NAME,
            ],
        );
        $sql = $this->getBuilder()->getInsertAllIntoTargetTableCommand(
            $fakeStage,
            $destination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'INSERT INTO "import_export_test_schema"."import_export_test_test" ("col1", "col2", "_timestamp") (SELECT IFF("col1" = \'\', NULL, "col1"),COALESCE("col2", \'\') AS "col2",\'2020-01-01 00:00:00\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src")',
            $sql,
        );
        $out = $this->connection->executeStatement($sql);
        self::assertEquals(4, $out);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
        }
    }

    public function testGetTruncateTableWithDeleteCommand(): void
    {
        $this->createTestSchema();
        $this->createStagingTableWithData();

        $ref = new SnowflakeTableReflection($this->connection, self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(3, $ref->getRowsCount());

        $sql = $this->getBuilder()->getTruncateTable(self::TEST_SCHEMA, self::TEST_STAGING_TABLE);
        self::assertEquals(
            'TRUNCATE TABLE "import_export_test_schema"."__temp_stagingTable"',
            $sql,
        );
        $this->connection->executeStatement($sql);
        self::assertEquals(0, $ref->getRowsCount());
    }

    public function testGetUpdateWithPkCommand(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is fewer columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $this->getDummyImportOptions(),
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = COALESCE("src"."col1", \'\'), "col2" = COALESCE("src"."col2", \'\') FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandRequireSameTables(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '1',
            ],
        ], $result);

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 0,
                requireSameTables: ImportOptions::SAME_TABLES_NOT_REQUIRED,
                nullManipulation: ImportOptions::NULL_MANIPULATION_SKIP, //<- skipp null manipulation
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = "src"."col1", "col2" = "src"."col2" FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = "src"."col1" ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandCasting(): void
    {
        $this->createTestSchema();
        $columnCollection = new ColumnCollection([
            $this->createNullableGenericColumn('pk1'),
            new SnowflakeColumn(
                'VARIANT',
                new Snowflake(
                    Snowflake::TYPE_VARIANT,
                ),
            ),
            new SnowflakeColumn(
                'BINARY',
                new Snowflake(
                    Snowflake::TYPE_BINARY,
                ),
            ),
            new SnowflakeColumn(
                'VARBINARY',
                new Snowflake(
                    Snowflake::TYPE_VARBINARY,
                ),
            ),
            new SnowflakeColumn(
                'OBJECT',
                new Snowflake(
                    Snowflake::TYPE_OBJECT,
                ),
            ),
            new SnowflakeColumn(
                'ARRAY',
                new Snowflake(
                    Snowflake::TYPE_ARRAY,
                ),
            ),
            new SnowflakeColumn(
                'VECTOR',
                new Snowflake(
                    Snowflake::TYPE_VECTOR,
                    [
                        'length' => 'INT,3',
                    ],
                ),
            ),
        ]);
        $destination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            $columnCollection,
            ['pk1'],
        );
        $stage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            $columnCollection,
            [],
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($destination),
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($stage),
        );

        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("pk1","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","VECTOR") 
SELECT \'1\', 
       TO_VARIANT(\'4.14\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT),
       ARRAY_CONSTRUCT(1, 2, 3, NULL),
       [1,2,3]::VECTOR(INT,3)
;',
            self::TEST_SCHEMA,
            self::TEST_TABLE,
        ));

        $this->connection->executeQuery(sprintf(
        /** @lang Snowflake */
            'INSERT INTO "%s"."%s" ("pk1","VARIANT","BINARY","VARBINARY","OBJECT","ARRAY","VECTOR") 
SELECT \'1\', 
       TO_VARIANT(\'3.14\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       TO_BINARY(HEX_ENCODE(\'1\'), \'HEX\'),
       OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT),
       ARRAY_CONSTRUCT(1, 2, 3, NULL),
       [1,2,3]::VECTOR(INT,3)
;',
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
        ));

        // no convert values no timestamp
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $stage,
            $destination,
            new SnowflakeImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 0,
                requireSameTables: ImportOptions::SAME_TABLES_NOT_REQUIRED,
                nullManipulation: ImportOptions::NULL_MANIPULATION_SKIP, //<- skipp null manipulation
                ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            ),
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "pk1" = "src"."pk1", "VARIANT" = "src"."VARIANT", "BINARY" = "src"."BINARY", "VARBINARY" = "src"."VARBINARY", "OBJECT" = "src"."OBJECT", "ARRAY" = "src"."ARRAY", "VECTOR" = "src"."VECTOR" FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."pk1" = "src"."pk1"  AND ("dest"."pk1" IS DISTINCT FROM "src"."pk1" OR "dest"."VARIANT" IS DISTINCT FROM "src"."VARIANT" OR "dest"."BINARY" IS DISTINCT FROM "src"."BINARY" OR "dest"."VARBINARY" IS DISTINCT FROM "src"."VARBINARY" OR "dest"."OBJECT" IS DISTINCT FROM "src"."OBJECT" OR "dest"."ARRAY" IS DISTINCT FROM "src"."ARRAY" OR "dest"."VECTOR" IS DISTINCT FROM "src"."VECTOR")',
            $sql,
        );
        $this->connection->executeStatement($sql);

        // now Snowflake return `vector = null` when you use `select * from …`, we need to cast vector explicitly here
        // this hack can be removed, when Snowflake start to select vectors correctly
        // Snowflake web console works properly, so this is probably bug in ODBC driver
        // We try driver version 3.4.0 and behaviour was the same
        $result = $this->connection->fetchAllAssociative(sprintf(
        // phpcs:ignore
            'SELECT "pk1", "VARIANT", "BINARY", "VARBINARY", "OBJECT", "ARRAY", cast("VECTOR" AS ARRAY) AS "VECTOR" FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'pk1' => '1',
                'VARIANT' => '"3.14"',
                'BINARY' => '1',
                'VARBINARY' => '1',
                'OBJECT' => <<<EOD
{
  "age": 42,
  "name": "Jones"
}
EOD,
                'ARRAY' => <<<EOD
[
  1,
  2,
  3,
  undefined
]
EOD,
                'VECTOR' => <<<EOD
[
  1,
  2,
  3
]
EOD,

            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValues(): void
    {
        $this->createTestSchema();
        $this->createTestTableWithColumns();
        $this->createStagingTableWithData(true);
        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'\',\'1\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2") VALUES (1,\'2\',\'\')',
                self::TEST_TABLE_IN_SCHEMA,
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
            ],
        ], $result);

        $options = new SnowflakeImportOptions(['col1']);

        // converver values
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            '2020-01-01 00:00:00',
        );
        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\') FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => null,
                'col2' => '',
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '2',
            ],
        ], $result);
    }

    public function testGetUpdateWithPkCommandConvertValuesWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'2\',\'\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '1',
                'col1' => '2',
                'col2' => '',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);

        // use timestamp
        $options = new SnowflakeImportOptions(['col1'], false, true);
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = IFF("src"."col1" = \'\', NULL, "src"."col1"), "col2" = COALESCE("src"."col2", \'\'), "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = COALESCE("src"."col1", \'\')  AND (COALESCE(TO_VARCHAR("dest"."col1"), \'\') != COALESCE("src"."col1", \'\') OR COALESCE(TO_VARCHAR("dest"."col2"), \'\') != COALESCE("src"."col2", \'\'))',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        foreach ($result as $item) {
            self::assertArrayHasKey('id', $item);
            self::assertArrayHasKey('col1', $item);
            self::assertArrayHasKey('col2', $item);
            self::assertArrayHasKey('_timestamp', $item);
            self::assertIsString($item['_timestamp']);
            self::assertSame(
                $timestampSet->format(DateTimeHelper::FORMAT),
                (new DateTime($item['_timestamp']))->format(DateTimeHelper::FORMAT),
            );
        }
    }

    public function testGetUpdateWithPkCommandNullManipulationWithTimestamp(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(false);

        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'1\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);

        // use timestamp
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: ['col1'],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: 0,
            requireSameTables: SnowflakeImportOptions::SAME_TABLES_REQUIRED,
            nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
        );
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals(
        // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = "src"."col1", "col2" = "src"."col2", "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = "src"."col1" ',
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        // timestamp was updated to $timestampSet but there is same row as in stage table so no other value is updated
        self::assertEquals([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $timestampSet->format(DateTimeHelper::FORMAT),
            ],
        ], $result);
    }

    public function nullManipulationWithTimestampFeatures(): Generator
    {
        yield 'default' => [
            'default',
            // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "col1" = "src"."col1", "col2" = "src"."col2", "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."col1" = "src"."col1"  AND ("dest"."col1" IS DISTINCT FROM "src"."col1" OR "dest"."col2" IS DISTINCT FROM "src"."col2")',
            true,
        ];
    }

    /**
     * @dataProvider nullManipulationWithTimestampFeatures
     */
    public function testGetUpdateWithPkCommandNullManipulationWithTimestampFeatures(
        string $feature,
        string $expectedSQL,
        bool $expectedTimestampChange,
    ): void {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $this->createTestTableWithColumns(true);
        $this->createStagingTableWithData(true);
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s.%s("pk1","pk2","col1","col2") VALUES (3,3,\'\',NULL)',
                self::TEST_SCHEMA_QUOTED,
                self::TEST_STAGING_TABLE_QUOTED,
            ),
        );

        // create fake destination and say that there is pk on col1
        $fakeDestination = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            ['col1'],
        );
        // create fake stage and say that there is less columns
        $fakeStage = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );

        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (1,\'1\',\'1\',\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );
        $this->connection->executeStatement(
            sprintf(
                'INSERT INTO %s("id","col1","col2","_timestamp") VALUES (3,\'3\',NULL,\'%s\')',
                self::TEST_TABLE_IN_SCHEMA,
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        self::assertEquals([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '3',
                'col1' => '3',
                'col2' => null,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);

        // use timestamp
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: 0,
            requireSameTables: SnowflakeImportOptions::SAME_TABLES_REQUIRED,
            nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            features: [$feature],
        );
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $fakeStage,
            $fakeDestination,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals(
            $expectedSQL,
            $sql,
        );
        $this->connection->executeStatement($sql);

        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        $assertTimestamp = $timestampInit;
        if ($expectedTimestampChange) {
            $assertTimestamp = $timestampSet;
        }

        // timestamp was updated to $timestampSet but there is same row as in stage table so no other value is updated
        self::assertEquals([
            [
                'id' => '1',
                'col1' => '1',
                'col2' => '1',
                '_timestamp' => $assertTimestamp->format(DateTimeHelper::FORMAT),
            ], // timestamp is not update when row has same value
            [
                'id' => '3',
                'col1' => '3',
                'col2' => null,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],  // timestamp is not update when row has same value and there is null
        ], $result);
    }

    public function testGetUpdateWithPkCommandNullManipulationSpecialDatatypes(): void
    {
        $timestampInit = new DateTime('2020-01-01 00:00:01');
        $timestampSet = new DateTime('2020-01-01 01:01:01');
        $this->createTestSchema();
        $tableDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'id',
                    new Snowflake(
                        Snowflake::TYPE_INT,
                    ),
                ),
                new SnowflakeColumn(
                    'array',
                    new Snowflake(
                        Snowflake::TYPE_ARRAY,
                        ['nullable' => true],
                    ),
                ),
                new SnowflakeColumn(
                    'vector',
                    new Snowflake(
                        Snowflake::TYPE_VECTOR,
                        [
                            'length' => 'INT,3',
                            'nullable' => true,
                        ],
                    ),
                ),
                new SnowflakeColumn(
                    'geometry',
                    new Snowflake(
                        Snowflake::TYPE_GEOMETRY,
                        ['nullable' => true],
                    ),
                ),
                new SnowflakeColumn(
                    'geography',
                    new Snowflake(
                        Snowflake::TYPE_GEOGRAPHY,
                        ['nullable' => true],
                    ),
                ),
                new SnowflakeColumn(
                    '_timestamp',
                    new Snowflake(Snowflake::TYPE_TIMESTAMP),
                ),
            ]),
            ['id'],
        );
        $stageDefinition = new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            false,
            new ColumnCollection([
                new SnowflakeColumn(
                    'id',
                    new Snowflake(
                        Snowflake::TYPE_INT,
                    ),
                ),
                new SnowflakeColumn(
                    'array',
                    new Snowflake(
                        Snowflake::TYPE_ARRAY,
                        ['nullable' => true],
                    ),
                ),
                new SnowflakeColumn(
                    'vector',
                    new Snowflake(
                        Snowflake::TYPE_VECTOR,
                        [
                            'length' => 'INT,3',
                            'nullable' => true,
                        ],
                    ),
                ),
                new SnowflakeColumn(
                    'geometry',
                    new Snowflake(
                        Snowflake::TYPE_GEOMETRY,
                        ['nullable' => true],
                    ),
                ),
                new SnowflakeColumn(
                    'geography',
                    new Snowflake(
                        Snowflake::TYPE_GEOGRAPHY,
                        ['nullable' => true],
                    ),
                ),
            ]),
            [],
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition($tableDefinition),
        );
        $this->connection->executeStatement(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommandFromDefinition(
                $stageDefinition,
            ),
        );

        $this->connection->executeStatement(
            sprintf(
            // phpcs:ignore
                'INSERT INTO %s.%s("id","array","vector","geometry","geography") SELECT 1,ARRAY_CONSTRUCT(2,\'arr\'),[1,2,3]::VECTOR(INT,3),\'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))\',\'POINT(-111.35 37.55)\'',
                SnowflakeQuote::quoteSingleIdentifier($stageDefinition->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($stageDefinition->getTableName()),
            ),
        );

        $this->connection->executeStatement(
            sprintf(
            // phpcs:ignore
                'INSERT INTO %s.%s("id","array","vector","geometry","geography","_timestamp") SELECT 1,ARRAY_CONSTRUCT(1,\'arr\'),[1,2,3]::VECTOR(INT,3),\'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))\',\'POINT(-122.35 37.55)\',\'%s\'',
                SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );
        $this->connection->executeStatement(
            sprintf(
            // phpcs:ignore
                'INSERT INTO %s.%s("id","array","vector","geometry","geography","_timestamp") SELECT 2,ARRAY_CONSTRUCT(1,\'arr\'),[1,2,3]::VECTOR(INT,3),\'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))\',\'POINT(-122.35 37.55)\',\'%s\'',
                SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
                $timestampInit->format(DateTimeHelper::FORMAT),
            ),
        );

        // now Snowflake return `vector = null` when you use `select * from …`, we need to cast vector explicitly here
        // this hack can be removed, when Snowflake start to select vectors correctly
        // Snowflake web console works properly, so this is probably bug in ODBC driver
        // We try driver version 3.4.0 and behaviour was the same
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT "id","array",CAST("vector" AS ARRAY) AS "vector" ,"geometry","geography","_timestamp" FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($tableDefinition->getTableName()),
        ));

        self::assertEquals([
            [
                'id' => '1',
                'array' => <<<EOD
[
  1,
  "arr"
]
EOD,
                'vector' => <<<EOD
[
  1,
  2,
  3
]
EOD,
                'geometry' => <<<EOD
{
  "coordinates": [
    [
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ]
    ]
  ],
  "type": "Polygon"
}
EOD,
                'geography' => <<<EOD
{
  "coordinates": [
    -122.35,
    37.55
  ],
  "type": "Point"
}
EOD,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '2',
                'array' => <<<EOD
[
  1,
  "arr"
]
EOD,
                'vector' => <<<EOD
[
  1,
  2,
  3
]
EOD,
                'geometry' => <<<EOD
{
  "coordinates": [
    [
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ]
    ]
  ],
  "type": "Polygon"
}
EOD,
                'geography' => <<<EOD
{
  "coordinates": [
    -122.35,
    37.55
  ],
  "type": "Point"
}
EOD,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);

        // use timestamp
        $options = new SnowflakeImportOptions(
            convertEmptyValuesToNull: [],
            isIncremental: false,
            useTimestamp: true,
            numberOfIgnoredLines: 0,
            requireSameTables: SnowflakeImportOptions::SAME_TABLES_REQUIRED,
            nullManipulation: SnowflakeImportOptions::NULL_MANIPULATION_SKIP,
            ignoreColumns: [ToStageImporterInterface::TIMESTAMP_COLUMN_NAME],
            features: ['native-types_timestamp-bc',],
        );
        $sql = $this->getBuilder()->getUpdateWithPkCommand(
            $stageDefinition,
            $tableDefinition,
            $options,
            $timestampSet->format(DateTimeHelper::FORMAT),
        );

        self::assertEquals(
            // phpcs:ignore
            'UPDATE "import_export_test_schema"."import_export_test_test" AS "dest" SET "id" = "src"."id", "array" = "src"."array", "vector" = "src"."vector", "geometry" = "src"."geometry", "geography" = "src"."geography", "_timestamp" = \'2020-01-01 01:01:01\' FROM "import_export_test_schema"."__temp_stagingTable" AS "src" WHERE "dest"."id" = "src"."id"  AND ("dest"."id" IS DISTINCT FROM "src"."id" OR "dest"."array" IS DISTINCT FROM "src"."array" OR "dest"."vector" IS DISTINCT FROM "src"."vector" OR ST_ASEWKT("dest"."geometry") IS DISTINCT FROM ST_ASEWKT("src"."geometry") OR ST_ASEWKT("dest"."geography") IS DISTINCT FROM ST_ASEWKT("src"."geography"))',
            $sql,
        );
        $this->connection->executeStatement($sql);

        // now Snowflake return `vector = null` when you use `select * from …`, we need to cast vector explicitly here
        // this hack can be removed, when Snowflake start to select vectors correctly
        // Snowflake web console works properly, so this is probably bug in ODBC driver
        // We try driver version 3.4.0 and behaviour was the same
        $result = $this->connection->fetchAllAssociative(sprintf(
            'SELECT "id","array",CAST("vector" AS ARRAY) AS "vector" ,"geometry","geography","_timestamp" FROM %s',
            self::TEST_TABLE_IN_SCHEMA,
        ));

        // timestamp was updated to $timestampSet but there is same row as in stage table so no other value is updated
        self::assertEquals([
            [
                'id' => '1',
                'array' => <<<EOD
[
  2,
  "arr"
]
EOD,
                'vector' => <<<EOD
[
  1,
  2,
  3
]
EOD,
                'geography' => <<<EOD
{
  "coordinates": [
    -111.35,
    37.55
  ],
  "type": "Point"
}
EOD,
                'geometry' => <<<EOD
{
  "coordinates": [
    [
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ]
    ]
  ],
  "type": "Polygon"
}
EOD,
                '_timestamp' => $timestampSet->format(DateTimeHelper::FORMAT),
            ],
            [
                'id' => '2',
                'array' => <<<EOD
[
  1,
  "arr"
]
EOD,
                'vector' => <<<EOD
[
  1,
  2,
  3
]
EOD,
                'geography' => <<<EOD
{
  "coordinates": [
    -122.35,
    37.55
  ],
  "type": "Point"
}
EOD,
                'geometry' => <<<EOD
{
  "coordinates": [
    [
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        0.000000000000000e+00
      ],
      [
        1.000000000000000e+01,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        1.000000000000000e+01
      ],
      [
        0.000000000000000e+00,
        0.000000000000000e+00
      ]
    ]
  ],
  "type": "Polygon"
}
EOD,
                '_timestamp' => $timestampInit->format(DateTimeHelper::FORMAT),
            ],
        ], $result);
    }

    private function getStagingTableDefinition(): SnowflakeTableDefinition
    {
        return new SnowflakeTableDefinition(
            self::TEST_SCHEMA,
            self::TEST_STAGING_TABLE,
            true,
            new ColumnCollection([
                $this->createNullableGenericColumn('pk1'),
                $this->createNullableGenericColumn('pk2'),
                $this->createNullableGenericColumn('col1'),
                $this->createNullableGenericColumn('col2'),
            ]),
            [],
        );
    }
}
