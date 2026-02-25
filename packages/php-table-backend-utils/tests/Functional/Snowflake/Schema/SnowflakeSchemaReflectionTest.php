<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Schema;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use RuntimeException;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;
use function PHPUnit\Framework\assertEquals;
use const PHP_EOL;

class SnowflakeSchemaReflectionTest extends SnowflakeBaseCase
{
    private SnowflakeSchemaReflection $schemaRef;

    public function setUp(): void
    {
        parent::setUp();
        $this->schemaRef = new SnowflakeSchemaReflection($this->connection, self::TEST_SCHEMA);

        $this->cleanSchema(self::TEST_SCHEMA);
    }

    public function testListTables(): void
    {
        $this->initTable();

        // create transient table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TRANSIENT TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('transient_table'),
            ),
        );

        // create temporary table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TEMPORARY TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('temporary_table'),
            ),
        );

        $tables = $this->schemaRef->getTablesNames();
        self::assertContains(self::TABLE_GENERIC, $tables);
        self::assertContains('transient_table', $tables);
        self::assertContains('temporary_table', $tables);
    }

    public function testListViews(): void
    {
        $this->initTable();

        $tableName = self::TABLE_GENERIC;
        $schemaName = self::TEST_SCHEMA;
        $viewName = self::VIEW_GENERIC;
        $sql = sprintf(
            '
CREATE VIEW %s.%s AS
     SELECT   "first_name",
              "last_name" 
     FROM %s.%s;
',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($viewName),
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
        $this->connection->executeQuery($sql);
        self::assertSame([$viewName], $this->schemaRef->getViewsNames());
    }

    public function testGetDefinitions(): void
    {
        $this->initTable();

        // create transient table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TRANSIENT TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('transient_table'),
            ),
        );

        // create temporary table
        $this->connection->executeQuery(
            sprintf(
                'CREATE OR REPLACE TEMPORARY TABLE %s.%s (
            "id" INTEGER,
    "first_name" VARCHAR(100),
    "last_name" VARCHAR(100)
);',
                SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                SnowflakeQuote::quoteSingleIdentifier('temporary_table'),
            ),
        );

        $tableName = self::TABLE_GENERIC;
        $schemaName = self::TEST_SCHEMA;
        $viewName = self::VIEW_GENERIC;
        $sql = sprintf(
            '
CREATE VIEW %s.%s AS
     SELECT   "first_name",
              "last_name" 
     FROM %s.%s;
',
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($viewName),
            SnowflakeQuote::quoteSingleIdentifier($schemaName),
            SnowflakeQuote::quoteSingleIdentifier($tableName),
        );
        $this->connection->executeQuery($sql);

        $definitions = $this->schemaRef->getDefinitions();

        self::assertCount(4, $definitions);

        $temporaryTableKey = 'temporary_table';
        self::assertEquals('temporary_table', $definitions[$temporaryTableKey]->getTableName());
        self::assertEquals(3, $definitions[$temporaryTableKey]->getColumnsDefinitions()->count());
        self::assertTrue($definitions[$temporaryTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$temporaryTableKey]->getTableType()->value);

        $transientTableKey = 'transient_table';
        self::assertEquals('transient_table', $definitions[$transientTableKey]->getTableName());
        self::assertEquals(3, $definitions[$transientTableKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$transientTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$transientTableKey]->getTableType()->value);

        $genericTableKey = self::TABLE_GENERIC;
        self::assertEquals(self::TABLE_GENERIC, $definitions[$genericTableKey]->getTableName());
        self::assertEquals(3, $definitions[$genericTableKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$genericTableKey]->isTemporary());
        self::assertEquals('table', $definitions[$genericTableKey]->getTableType()->value);

        $genericViewKey = self::VIEW_GENERIC;
        self::assertEquals(self::VIEW_GENERIC, $definitions[$genericViewKey]->getTableName());
        self::assertEquals(2, $definitions[$genericViewKey]->getColumnsDefinitions()->count());
        self::assertFalse($definitions[$genericViewKey]->isTemporary());
        self::assertEquals('view', $definitions[$genericViewKey]->getTableType()->value);
    }

    public function testGetDefinitionsWithEmptySchema(): void
    {
        $this->createSchema(self::TEST_SCHEMA);
        $definitions = $this->schemaRef->getDefinitions();

        self::assertCount(0, $definitions);
    }

    public function testGetDefinitionsPrimaryKeysNotDuplicatedAcrossSchemas(): void
    {
        $otherSchema = self::TESTS_PREFIX . 'otherSchema';
        $this->cleanSchema($otherSchema);
        $this->createSchema(self::TEST_SCHEMA);
        $this->createSchema($otherSchema);

        try {
            $tableName = 'TABLENAME';

            // create table with primary key in the target schema
            $this->connection->executeQuery(
                sprintf(
                    'CREATE TABLE %s.%s ("ID_SRC" INTEGER, PRIMARY KEY ("ID_SRC"))',
                    SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ),
            );

            // create table with same name and same primary key in a different schema
            $this->connection->executeQuery(
                sprintf(
                    'CREATE TABLE %s.%s ("ID_SRC" INTEGER, PRIMARY KEY ("ID_SRC"))',
                    SnowflakeQuote::quoteSingleIdentifier($otherSchema),
                    SnowflakeQuote::quoteSingleIdentifier($tableName),
                ),
            );

            // create a second table in the target schema with same PK column name
            $backupTableName = 'TABLENAME_BACKUP';
            $this->connection->executeQuery(
                sprintf(
                    'CREATE TABLE %s.%s ("ID_SRC" INTEGER, PRIMARY KEY ("ID_SRC"))',
                    SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
                    SnowflakeQuote::quoteSingleIdentifier($backupTableName),
                ),
            );

            $definitions = $this->schemaRef->getDefinitions();

            self::assertCount(2, $definitions);

            // TABLENAME must have exactly one primary key, not duplicated from other schema
            self::assertSame(['ID_SRC'], $definitions[$tableName]->getPrimaryKeysNames());

            // TABLENAME_BACKUP must also have exactly one primary key
            self::assertSame(['ID_SRC'], $definitions[$backupTableName]->getPrimaryKeysNames());
        } finally {
            $this->cleanSchema($otherSchema);
        }
    }

    public function testGetDefinitionsCoversAllTypes(): void
    {
        $this->createSchema(self::TEST_SCHEMA);

        $createTableQuery = $this->createTableWithAllSupportedTypes('all_types_table');
        $this->connection->executeQuery($createTableQuery);

        $definitions = $this->schemaRef->getDefinitions();

        $columnsReflection = [];
        /** @var SnowflakeColumn $columnReflection */
        foreach ($definitions['all_types_table']->getColumnsDefinitions() as $columnReflection) {
            $columnName = $columnReflection->getColumnName();
            $columnSql = $columnReflection->getColumnDefinition()->getTypeOnlySQLDefinition();
            $columnsReflection[$columnName] = $columnSql;
        }

        $this->assertEquals(
            [
                'col_number_default'          => 'NUMBER (38,0)',
                'col_number_9'                => 'NUMBER (9,0)',
                'col_number_9_2'              => 'NUMBER (9,2)',
                'col_dec_default'             => 'NUMBER (38,0)',
                'col_decimal_default'         => 'NUMBER (38,0)',
                'col_numeric_default'         => 'NUMBER (38,0)',
                'col_int_default'             => 'NUMBER (38,0)',
                'col_integer_default'         => 'NUMBER (38,0)',
                'col_bigint_default'          => 'NUMBER (38,0)',
                'col_smallint_default'        => 'NUMBER (38,0)',
                'col_tinyint_default'         => 'NUMBER (38,0)',
                'col_byteint_default'         => 'NUMBER (38,0)',
                'col_float_default'           => 'FLOAT',
                'col_float4_default'          => 'FLOAT',
                'col_float8_default'          => 'FLOAT',
                'col_double_default'          => 'FLOAT',
                'col_double_precision_default'=> 'FLOAT',
                'col_real_default'            => 'FLOAT',
                'col_varchar_default'         => 'VARCHAR (16777216)',
                'col_varchar_50'              => 'VARCHAR (50)',
                'col_varchar_100'             => 'VARCHAR (100)',
                'col_char_default'            => 'VARCHAR (1)',
                'col_char_10'                 => 'VARCHAR (10)',
                'col_char_20'                 => 'VARCHAR (20)',
                'col_character_default'       => 'VARCHAR (1)',
                'col_character_20'            => 'VARCHAR (20)',
                'col_char_varying_default'    => 'VARCHAR (16777216)',
                'col_character_varying_default'=> 'VARCHAR (16777216)',
                'col_string_default'          => 'VARCHAR (16777216)',
                'col_text_default'            => 'VARCHAR (16777216)',
                'col_nchar_varying_default'   => 'VARCHAR (16777216)',
                'col_nchar_default'           => 'VARCHAR (1)',
                'col_nvarchar_default'        => 'VARCHAR (16777216)',
                'col_nvarchar2_default'       => 'VARCHAR (16777216)',
                'col_boolean_default'         => 'BOOLEAN',
                'col_date_default'            => 'DATE',
                'col_datetime_default'        => 'TIMESTAMP_NTZ (9)',
                'col_time_default'            => 'TIME (9)',
                'col_time_3'                  => 'TIME (3)',
                'col_timestamp_default'       => 'TIMESTAMP_NTZ (9)',
                'col_timestamp_ntz_default'   => 'TIMESTAMP_NTZ (9)',
                'col_timestamp_ntz_6'         => 'TIMESTAMP_NTZ (6)',
                'col_timestamp_ltz_default'   => 'TIMESTAMP_LTZ (9)',
                'col_timestamp_ltz_6'         => 'TIMESTAMP_LTZ (6)',
                'col_timestamp_tz_default'    => 'TIMESTAMP_TZ (9)',
                'col_timestamp_tz_6'          => 'TIMESTAMP_TZ (6)',
                'col_variant_default'         => 'VARIANT',
                'col_binary_default'          => 'BINARY (8388608)',
                'col_varbinary_default'       => 'BINARY (8388608)',
                'col_object_default'          => 'OBJECT',
                'col_array_default'           => 'ARRAY',
                'col_geography_default'       => 'GEOGRAPHY',
                'col_geometry_default'        => 'GEOMETRY',
                'col_vector_int_10'           => 'VECTOR (INT, 10)',
                'col_vector_float_15'         => 'VECTOR (FLOAT, 15)',
            ],
            $columnsReflection,
        );
    }

    private function createTableWithAllSupportedTypes(string $tableName): string
    {
        $typesDefinitions = [];
        foreach (Snowflake::TYPES as $type) {
            $typesDefinitions += $this->createTypeDefinition($type);
        }

        return sprintf(
            'CREATE TABLE %s (%s);',
            SnowflakeQuote::quoteSingleIdentifier($tableName),
            PHP_EOL . implode(', ' . PHP_EOL, $typesDefinitions),
        );
    }

    /**
     * @return array<string, string>
     */
    private function createTypeDefinition(string $typeName): array
    {
        $definitions = [];

        switch ($typeName) {
            // Numeric types
            case Snowflake::TYPE_NUMBER:
                $definitions['col_number_default'] = '"col_number_default"           NUMBER';
                $definitions['col_number_9']       = '"col_number_9"                 NUMBER(9)';
                $definitions['col_number_9_2']     = '"col_number_9_2"               NUMBER(9,2)';
                break;
            case Snowflake::TYPE_DEC:
                $definitions['col_dec_default'] = '"col_dec_default"              DEC';
                break;
            case Snowflake::TYPE_DECIMAL:
                $definitions['col_decimal_default'] = '"col_decimal_default"          DECIMAL';
                break;
            case Snowflake::TYPE_NUMERIC:
                $definitions['col_numeric_default'] = '"col_numeric_default"          NUMERIC';
                break;
            case Snowflake::TYPE_INT:
                $definitions['col_int_default'] = '"col_int_default"              INT';
                break;
            case Snowflake::TYPE_INTEGER:
                $definitions['col_integer_default'] = '"col_integer_default"          INTEGER';
                break;
            case Snowflake::TYPE_BIGINT:
                $definitions['col_bigint_default'] = '"col_bigint_default"           BIGINT';
                break;
            case Snowflake::TYPE_SMALLINT:
                $definitions['col_smallint_default'] = '"col_smallint_default"         SMALLINT';
                break;
            case Snowflake::TYPE_TINYINT:
                $definitions['col_tinyint_default'] = '"col_tinyint_default"          TINYINT';
                break;
            case Snowflake::TYPE_BYTEINT:
                $definitions['col_byteint_default'] = '"col_byteint_default"          BYTEINT';
                break;
            case Snowflake::TYPE_FLOAT:
                $definitions['col_float_default'] = '"col_float_default"            FLOAT';
                break;
            case Snowflake::TYPE_FLOAT4:
                $definitions['col_float4_default'] = '"col_float4_default"           FLOAT4';
                break;
            case Snowflake::TYPE_FLOAT8:
                $definitions['col_float8_default'] = '"col_float8_default"           FLOAT8';
                break;
            case Snowflake::TYPE_DOUBLE:
                $definitions['col_double_default'] = '"col_double_default"           DOUBLE';
                break;
            case Snowflake::TYPE_DOUBLE_PRECISION:
                $definitions['col_double_precision_default'] = '"col_double_precision_default" DOUBLE PRECISION';
                break;
            case Snowflake::TYPE_REAL:
                $definitions['col_real_default'] = '"col_real_default"             REAL';
                break;

            // String types
            case Snowflake::TYPE_VARCHAR:
                $definitions['col_varchar_default'] = '"col_varchar_default"          VARCHAR';
                $definitions['col_varchar_50']      = '"col_varchar_50"               VARCHAR(50)';
                $definitions['col_varchar_100']     = '"col_varchar_100"              VARCHAR(100)';
                break;
            case Snowflake::TYPE_CHAR:
                $definitions['col_char_default'] = '"col_char_default"             CHAR';
                $definitions['col_char_10']      = '"col_char_10"                  CHAR(10)';
                $definitions['col_char_20']      = '"col_char_20"                  CHAR(20)';
                break;
            case Snowflake::TYPE_CHARACTER:
                $definitions['col_character_default'] = '"col_character_default"        CHARACTER';
                $definitions['col_character_20']      = '"col_character_20"             CHARACTER(20)';
                break;
            case Snowflake::TYPE_CHAR_VARYING:
                $definitions['col_char_varying_default'] = '"col_char_varying_default"     CHAR VARYING';
                break;
            case Snowflake::TYPE_CHARACTER_VARYING:
                $definitions['col_character_varying_default'] = '"col_character_varying_default"  CHARACTER VARYING';
                break;
            case Snowflake::TYPE_STRING:
                $definitions['col_string_default'] = '"col_string_default"           STRING';
                break;
            case Snowflake::TYPE_TEXT:
                $definitions['col_text_default'] = '"col_text_default"             TEXT';
                break;
            case Snowflake::TYPE_NCHAR_VARYING:
                $definitions['col_nchar_varying_default'] = '"col_nchar_varying_default"    NCHAR VARYING';
                break;
            case Snowflake::TYPE_NCHAR:
                $definitions['col_nchar_default'] = '"col_nchar_default"            NCHAR';
                break;
            case Snowflake::TYPE_NVARCHAR:
                $definitions['col_nvarchar_default'] = '"col_nvarchar_default"         NVARCHAR';
                break;
            case Snowflake::TYPE_NVARCHAR2:
                $definitions['col_nvarchar2_default'] = '"col_nvarchar2_default"        NVARCHAR2';
                break;

            // Boolean
            case Snowflake::TYPE_BOOLEAN:
                $definitions['col_boolean_default'] = '"col_boolean_default"          BOOLEAN';
                break;

            // Date/Time types
            case Snowflake::TYPE_DATE:
                $definitions['col_date_default'] = '"col_date_default"             DATE';
                break;
            case Snowflake::TYPE_DATETIME:
                $definitions['col_datetime_default'] = '"col_datetime_default"         DATETIME';
                break;
            case Snowflake::TYPE_TIME:
                $definitions['col_time_default'] = '"col_time_default"             TIME';
                $definitions['col_time_3']     = '"col_time_3"                   TIME(3)';
                break;
            case Snowflake::TYPE_TIMESTAMP:
                $definitions['col_timestamp_default'] = '"col_timestamp_default"        TIMESTAMP';
                break;
            case Snowflake::TYPE_TIMESTAMP_NTZ:
                $definitions['col_timestamp_ntz_default'] = '"col_timestamp_ntz_default"    TIMESTAMP_NTZ';
                $definitions['col_timestamp_ntz_6']       = '"col_timestamp_ntz_6"          TIMESTAMP_NTZ(6)';
                break;
            case Snowflake::TYPE_TIMESTAMP_LTZ:
                $definitions['col_timestamp_ltz_default'] = '"col_timestamp_ltz_default"    TIMESTAMP_LTZ';
                $definitions['col_timestamp_ltz_6']       = '"col_timestamp_ltz_6"          TIMESTAMP_LTZ(6)';
                break;
            case Snowflake::TYPE_TIMESTAMP_TZ:
                $definitions['col_timestamp_tz_default'] = '"col_timestamp_tz_default"     TIMESTAMP_TZ';
                $definitions['col_timestamp_tz_6']       = '"col_timestamp_tz_6"           TIMESTAMP_TZ(6)';
                break;

            // Semi-structured & Other types
            case Snowflake::TYPE_VARIANT:
                $definitions['col_variant_default'] = '"col_variant_default"          VARIANT';
                break;
            case Snowflake::TYPE_BINARY:
                $definitions['col_binary_default'] = '"col_binary_default"           BINARY';
                break;
            case Snowflake::TYPE_VARBINARY:
                $definitions['col_varbinary_default'] = '"col_varbinary_default"        VARBINARY';
                break;
            case Snowflake::TYPE_OBJECT:
                $definitions['col_object_default'] = '"col_object_default"           OBJECT';
                break;
            case Snowflake::TYPE_ARRAY:
                $definitions['col_array_default'] = '"col_array_default"            ARRAY';
                break;
            case Snowflake::TYPE_GEOGRAPHY:
                $definitions['col_geography_default'] = '"col_geography_default"        GEOGRAPHY';
                break;
            case Snowflake::TYPE_GEOMETRY:
                $definitions['col_geometry_default'] = '"col_geometry_default"         GEOMETRY';
                break;
            case Snowflake::TYPE_VECTOR:
                $definitions['col_vector_int_10'] = '"col_vector_int_10"           VECTOR(INT, 10)';
                $definitions['col_vector_float_15'] = '"col_vector_float_15"           VECTOR(FLOAT, 15)';
                break;
            default:
                throw new RuntimeException(sprintf('Unsupported Snowflake type "%s"', $typeName));
        }

        return $definitions;
    }
}
