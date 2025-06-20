<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Snowflake;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class SnowflakeColumn implements ColumnInterface
{
    private string $columnName;

    private Snowflake $columnDefinition;

    public function __construct(string $columnName, Snowflake $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    public static function createGenericColumn(string $columnName): SnowflakeColumn
    {
        $definition = new Snowflake(
            Snowflake::TYPE_VARCHAR,
            [
                'nullable' => false,
                'default' => '\'\'',
            ],
        );

        return new self(
            $columnName,
            $definition,
        );
    }

    public function getColumnName(): string
    {
        return $this->columnName;
    }

    /**
     * @return Snowflake
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }

    /**
     * @param array{
     *     name: string,
     *     type: string,
     *     default: string,
     *     "null?": string,
     * } $dbResponse
     */
    public static function createFromDB(array $dbResponse): SnowflakeColumn
    {
        $type = $dbResponse['type'];
        $default = $dbResponse['default'];
        $length = null;

        $info = self::extractTypeAndLengthFromDB($dbResponse['type']);
        if ($info !== null) {
            $type = $info['type'];
            $length = $info['length'];
        }

        return new self($dbResponse['name'], new Snowflake(
            $type,
            [
                'nullable' => $dbResponse['null?'] === 'Y',
                'length' => $length,
                'default' => $default,
            ],
        ));
    }

    /**
     * @return array{type: string, length: string}|null
     */
    public static function extractTypeAndLengthFromDB(string $dbType): ?array
    {
        $matches = [];
        if (preg_match('/^(?<type>\w+)\((?<length>[a-zA-Z0-9, ]+)\).*$/ui', $dbType, $matches)) {
            $type = $matches['type'];
            $length = $matches['length'];

            return ['type' => $type, 'length' => $length];
        }

        return null;
    }

    public static function createTimestampColumn(string $columnName = self::TIMESTAMP_COLUMN_NAME): SnowflakeColumn
    {
        return new self(
            $columnName,
            new Snowflake(
                Snowflake::TYPE_TIMESTAMP_NTZ,
            ),
        );
    }
}
