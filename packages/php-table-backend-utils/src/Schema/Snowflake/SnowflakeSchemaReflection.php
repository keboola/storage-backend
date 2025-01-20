<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema\Snowflake;

use Doctrine\DBAL\Connection;
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
            'FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'%s\' ORDER BY TABLE_NAME;',
            $this->schemaName,
        );

        $columnsQuery = sprintf(
            'SELECT TABLE_NAME, '.
            'COLUMN_NAME AS "name", DATA_TYPE AS "type", COLUMN_DEFAULT AS "default", IS_NULLABLE AS "null?" '.
            'FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = \'%s\' ORDER BY TABLE_NAME;',
            $this->schemaName,
        );

        $primaryKeyQuery = sprintf(
            'SHOW PRIMARY KEYS',
        );

        /** @var array<int, array{TABLE_NAME: string, TABLE_TYPE: string, BYTES: int, ROW_COUNT: int}> $informations */
        $informations = $this->connection->fetchAllAssociative($informationsQuery);
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
}
