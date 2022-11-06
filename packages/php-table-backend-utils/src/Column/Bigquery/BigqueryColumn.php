<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\TableBackendUtils\Column\ColumnInterface;

class BigqueryColumn implements ColumnInterface
{
    private string $columnName;

    private Bigquery $columnDefinition;

    public function __construct(string $columnName, Bigquery $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    public function getColumnName(): string
    {
        return $this->columnName;
    }

    /** @return Bigquery */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }

    public static function createGenericColumn(string $columnName): BigqueryColumn
    {
        $definition = new Bigquery(
            Bigquery::TYPE_STRING,
            [
                'nullable' => false,
                'default' => '\'\'',
            ]
        );

        return new self(
            $columnName,
            $definition
        );
    }

    public static function createTimestampColumn(string $columnName = self::TIMESTAMP_COLUMN_NAME): BigqueryColumn
    {
        return new self(
            $columnName,
            new Bigquery(
                Bigquery::TYPE_TIMESTAMP,
            )
        );
    }

    /**
     * @param array{
     *  table_catalog: string,
     *  table_schema: string,
     *  table_name: string,
     *  column_name: string,
     *  ordinal_position: int,
     *  is_nullable: string,
     *  data_type: string,
     *  is_hidden: string,
     *  is_system_defined: string,
     *  is_partitioning_column: string,
     *  clustering_ordinal_position: ?string,
     *  collation_name: string,
     *  column_default: string,
     *  rounding_mode: ?string,
     * } $dbResponse
     */
    public static function createFromDB(array $dbResponse): BigqueryColumn
    {
        $type = $dbResponse['data_type'];
        $default = $dbResponse['column_default'] === 'NULL' ? null : $dbResponse['column_default'];
        $length = null;

        $matches = [];
        if (preg_match('/^(\w+)\(([0-9\, ]+)\)$/ui', $dbResponse['data_type'], $matches)) {
            $type = $matches[1];
            $length = str_replace(' ', '', $matches[2]);
        }

        /** @var array{length?:string|null, nullable?:bool, default?:string|null} $options */
        $options = [
            'nullable' => $dbResponse['is_nullable'] === 'YES',
            'length' => $length,
            'default' => $default,
        ];
        return new self($dbResponse['column_name'], new Bigquery(
            $type,
            $options
        ));
    }
}
