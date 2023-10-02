<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery;

use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\TableBackendUtils\Column\ColumnInterface;

/**
 * @phpstan-import-type BigqueryTableFieldSchema from Bigquery
 */
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
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    public static function createFromDB(array $dbResponse): BigqueryColumn
    {
        return new self(
            $dbResponse['name'],
            RESTtoSQLDatatypeConverter::createFromDB($dbResponse)
        );
    }
}
