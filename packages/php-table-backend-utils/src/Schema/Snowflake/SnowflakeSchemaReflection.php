<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\SchemaReflectionInterface;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\TableType;
use RuntimeException;

final class SnowflakeSchemaReflection implements SchemaReflectionInterface
{
    private Connection $connection;

    private string $schemaName;

    public function __construct(Connection $connection, string $schemaName)
    {
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getTablesNames(): array
    {
        /** @var array<array{name:string,kind:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW TABLES IN SCHEMA %s',
                SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
            ),
        );

        return array_map(static fn($table) => $table['name'], $tables);
    }

    /**
     * @return string[]
     */
    public function getViewsNames(): array
    {
        /** @var array<array{name:string}> $tables */
        $tables = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW VIEWS IN SCHEMA %s',
                SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
            ),
        );

        return array_map(static fn($table) => $table['name'], $tables);
    }

    /**
     * @return array<string, SnowflakeTableDefinition>
     */
    public function getDefinitions(): array
    {
        $informationsQuery = sprintf(
            'SELECT TABLE_NAME, TABLE_TYPE, BYTES, ROW_COUNT '.
            'FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME;',
            SnowflakeQuote::quote($this->schemaName),
        );

        // Snowflake maps in DESC TABLE few data-type aliases to their basic types
        // but in INFORMATION_SCHEMA.COLUMNS table keep data-type aliases
        // here is implemented same mapping as DESC TABLE uses
        $columnsQuery = sprintf(
            <<<SQL
SELECT 
    TABLE_NAME,
    COLUMN_NAME AS "name",
    CASE 
        -- Map string types to VARCHAR
        WHEN DATA_TYPE IN ('CHAR', 'VARCHAR', 'STRING', 'TEXT') THEN 
            'VARCHAR(' || COALESCE(CHARACTER_MAXIMUM_LENGTH::STRING, '16777216') || ')'
        
        -- Map numeric types to NUMBER
        WHEN DATA_TYPE IN ('NUMBER', 'DECIMAL', 'NUMERIC') THEN 
            'NUMBER(' || COALESCE(NUMERIC_PRECISION::STRING, '') || ',' || COALESCE(NUMERIC_SCALE::STRING, '0') || ')'
        
        -- Map date and time types to their respective names - DATE is mapped directly to DATE
        WHEN DATA_TYPE IN ('DATETIME', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ') THEN 
            DATA_TYPE || '(' || COALESCE(DATETIME_PRECISION::STRING, '') || ')'
        
        -- Map binary and varbinary - Snowflake don't have length for binary in INFORMATION_SCHEMA - handled in code
        WHEN DATA_TYPE IN ('BINARY', 'VARBINARY') THEN
            'BINARY' 
 
        -- Default case for all other types as they are mapped by DESC TABLE direcly to themself
        ELSE DATA_TYPE
    END AS "type",
    COLUMN_DEFAULT AS "default",
    IS_NULLABLE AS "null?"
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = %s
ORDER BY TABLE_NAME, ORDINAL_POSITION;
SQL,
            SnowflakeQuote::quote($this->schemaName),
        );

        $primaryKeyQuery = sprintf(
            'SHOW PRIMARY KEYS',
        );

        /** @var array<int, array{TABLE_NAME: string, TABLE_TYPE: string, BYTES: int, ROW_COUNT: int}> $informations */
        $informations = $this->connection->fetchAllAssociative($informationsQuery);

        // short-circuit > no tables no need to continue
        if (count($informations) === 0) {
            return [];
        }

        /** @var array<int, array{TABLE_NAME: string, name: string, type: string, default: string, null?: string}> $columns */
        $columns = $this->connection->fetchAllAssociative($columnsQuery);
        /** @var array<int, array{
         *     created_on: string,
         *     database_name: string,
         *     schema_name: string,
         *     table_name: string,
         *     column_name: string,
         *     key_sequence: int,
         *     constraint_name: string,
         *     rely: bool,
         *     comment: ?string}> $primaryKeys */
        $primaryKeys = $this->connection->fetchAllAssociative($primaryKeyQuery);

        $tables = [];

        foreach ($informations as $information) {
            $tables[$information['TABLE_NAME']]['PROPS'] = $information;

            switch (strtoupper($information['TABLE_TYPE'])) {
                case 'BASE TABLE':
                    $tables[$information['TABLE_NAME']]['PROPS']['TEMPORARY'] = false;
                    $tables[$information['TABLE_NAME']]['PROPS']['TABLE_TYPE'] = TableType::TABLE;
                    break;
                case 'EXTERNAL TABLE':
                    $tables[$information['TABLE_NAME']]['PROPS']['TEMPORARY'] = false;
                    $tables[$information['TABLE_NAME']]['PROPS']['TABLE_TYPE'] = TableType::SNOWFLAKE_EXTERNAL;
                    break;
                case 'LOCAL TEMPORARY':
                case 'TEMPORARY TABLE':
                    $tables[$information['TABLE_NAME']]['PROPS']['TEMPORARY'] = true;
                    $tables[$information['TABLE_NAME']]['PROPS']['TABLE_TYPE'] = TableType::TABLE;
                    break;
                case 'VIEW':
                    $tables[$information['TABLE_NAME']]['PROPS']['TEMPORARY'] = false;
                    $tables[$information['TABLE_NAME']]['PROPS']['TABLE_TYPE'] = TableType::VIEW;
                    break;
                default:
                    throw new RuntimeException(sprintf(
                        'Table type "%s" is not known.',
                        $information['TABLE_TYPE'],
                    ));
            }
        }

        foreach ($columns as $column) {
            // Offset 'null?' does not exist on
            // array{TABLE_NAME: string, name: string, type: string, default: string, null?: string}.
            // @phpstan-ignore-next-line
            $column['null?'] = ($column['null?'] === 'YES' ? 'Y' : 'N');

            // Snowflake have length for binary columns only in DESC TABLE or SHOW COLUMNS
// disabled for now
//            if ($column['type'] === Snowflake::TYPE_BINARY) {
//                $info = $this->getBinaryColumnLength($column['TABLE_NAME'], $column['name']);
//                if ($info !== null) {
//                    $column['type'] = sprintf(
//                        '%s(%s)',
//                        Snowflake::TYPE_BINARY,
//                        $info['length'],
//                    );
//                }
//            }
            $tables[$column['TABLE_NAME']]['COLUMNS'][] = SnowflakeColumn::createFromDB($column);
        }

        foreach ($primaryKeys as $primaryKey) {
            $tables[$primaryKey['table_name']]['PRIMARY_KEYS'][] = $primaryKey['column_name'];
        }

        $definitions = [];
        foreach ($tables as $tableName => $table) {
            $definitions[$tableName] = new SnowflakeTableDefinition(
                $this->schemaName,
                $tableName,
                $table['PROPS']['TEMPORARY'],
                new ColumnCollection($table['COLUMNS'] ?? []),
                $table['PRIMARY_KEYS'] ?? [],
                $table['PROPS']['TABLE_TYPE'],
            );
        }
        return $definitions;
    }

    /**
     * @return array{type: string, length: string}|null
     */
    private function getBinaryColumnLength(string $tableName, string $columnName): ?array
    {
        $sql = sprintf('DESCRIBE TABLE %s', SnowflakeQuote::quote($tableName));
        /** @var array<
         *     int,
         *      array{
         *          name: string,
         *          type: string,
         *          kind: string,
         *          null?: string,
         *          default: ?string,
         *          "primary key": string,
         *          "unique key": string,
         *          check: ?string,
         *          expression: ?string,
         *          comment: ?string,
         *          "policy name": ?string,
         *          "privacy domain": ?string
         *      }
         * > $columns
         */
        $columns = $this->connection->fetchAllAssociative($sql);
        foreach ($columns as $column) {
            if ($column['name'] === $columnName) {
                return SnowflakeColumn::extractTypeAndLengthFromDB($column['type']);
            }
        }

        return null;
    }
}
