<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Exasol;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class ExasolColumn implements ColumnInterface
{
    private string $columnName;

    private Exasol $columnDefinition;

    public function __construct(string $columnName, Exasol $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    public static function createGenericColumn(string $columnName): ExasolColumn
    {
        $definition = new Exasol(
            Exasol::TYPE_VARCHAR,
            [
                'length' => '2000000',
                'nullable' => false,
                'default' => '\'\'',
            ]
        );

        return new self(
            $columnName,
            $definition
        );
    }

    public function getColumnName(): string
    {
        return $this->columnName;
    }

    /**
     * @return Exasol
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }

    /**
     * @param array{
     *  COLUMN_DEFAULT: ?string,
     *  COLUMN_NAME: string,
     *  TYPE_NAME: string,
     *  COLUMN_IS_NULLABLE: string,
     *  COLUMN_TYPE: string,
     *  TYPE_NAME: string,
     *  COLUMN_NUM_PREC: ?string,
     *  COLUMN_NUM_SCALE: ?string,
     *  COLUMN_MAXSIZE: ?string,
     * } $dbResponse
     */
    public static function createFromDB(array $dbResponse): ExasolColumn
    {
        $defaultValue = trim((string) $dbResponse['COLUMN_DEFAULT']);

        return new ExasolColumn(
            $dbResponse['COLUMN_NAME'],
            new Exasol(
                $dbResponse['TYPE_NAME'],
                [
                    'length' => self::extractColumnLength($dbResponse),
                    'nullable' => $dbResponse['COLUMN_IS_NULLABLE'] === '1',
                    'default' => $defaultValue === '' ? null : $defaultValue,
                ]
            )
        );
    }

    /**
     * @param array{
     *  COLUMN_TYPE: string,
     *  TYPE_NAME: string,
     *  COLUMN_NUM_PREC: ?string,
     *  COLUMN_NUM_SCALE: ?string,
     *  COLUMN_MAXSIZE: ?string,
     * } $colData
     */
    private static function extractColumnLength(array $colData): string
    {
        $colType = $colData['COLUMN_TYPE'];
        if ($colData['TYPE_NAME'] === Exasol::TYPE_INTERVAL_DAY_TO_SECOND) {
            preg_match('/INTERVAL DAY\((?P<valDays>\d)\) TO SECOND\((?P<valSeconds>\d)\)/', $colType, $output_array);
            return $output_array['valDays'] . ',' . $output_array['valSeconds'];
        }

        if ($colData['TYPE_NAME'] === Exasol::TYPE_INTERVAL_YEAR_TO_MONTH) {
            preg_match('/INTERVAL YEAR\((?P<val>\d)\) TO MONTH/', $colType, $output_array);
            return $output_array['val'];
        }

        $precision = $colData['COLUMN_NUM_PREC'];
        $scale = $colData['COLUMN_NUM_SCALE'];
        $maxLength = $colData['COLUMN_MAXSIZE'];
        if ($precision === null && $scale === null) {
            return $maxLength ?? '';
        }
        return "{$precision},{$scale}";
    }
}
