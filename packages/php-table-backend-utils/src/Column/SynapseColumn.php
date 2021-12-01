<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Synapse;

final class SynapseColumn implements ColumnInterface
{
    /** @var string */
    private $columnName;

    /** @var Synapse */
    private $columnDefinition;

    public function __construct(string $columnName, Synapse $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    /**
     * @param array{
     *     column_name:string,
     *     column_type:string,
     *     column_length:string,
     *     column_precision:string,
     *     column_scale:string,
     *     column_is_nullable:string,
     *     column_default:?string
     * } $dbResponse
     */
    public static function createFromDB(array $dbResponse): ColumnInterface
    {
        $type = strtoupper($dbResponse['column_type']);
        $length = $dbResponse['column_length'];
        $default = $dbResponse['column_default'];
        if ($dbResponse['column_length'] === 'null') {
            $length = null;
        }
        if ($length === '-1' && in_array($type, [
                Synapse::TYPE_VARCHAR,
                Synapse::TYPE_NVARCHAR,
                Synapse::TYPE_VARBINARY,
            ], true)) {
            $length = 'MAX';
        } elseif (in_array($type, [
            Synapse::TYPE_NCHAR,
            Synapse::TYPE_NVARCHAR,
        ], true)) {
            // types with bite-pairs
            $length = (string) ((int) $length / 2);
        }

        if (in_array($type, Synapse::TYPES_WITH_COMPLEX_LENGTH, true)) {
            // types with complex definition precision and scale (1,0)
            $length = $dbResponse['column_precision'] . ',' . $dbResponse['column_scale'];
        }
        if (in_array($type, Synapse::TYPES_WITHOUT_LENGTH, true)) {
            // types with no length definition
            $length = null;
        }
        if ($type === Synapse::TYPE_FLOAT) {
            $length = $dbResponse['column_precision'];
        }
        if (in_array($type, [
            Synapse::TYPE_DATETIMEOFFSET,
            Synapse::TYPE_DATETIME2,
            Synapse::TYPE_TIME,
        ], true)) {
            // scale is same as length in definition
            $length = $dbResponse['column_scale'];
        }

        if (isset($default)) {
            $default = preg_replace(
                '/^\((.+)\)$/',
                '\\1',
                (string) preg_replace(
                    '/^\(\((.+)\)\)$/',
                    '\\1',
                    $default
                )
            );
        }

        $definition = new Synapse(
            $type,
            [
                'nullable' => strtolower($dbResponse['column_is_nullable']) === '1',
                'length' => $length,
                'default' => $default,
            ]
        );

        return new self($dbResponse['column_name'], $definition);
    }

    /**
     * @return SynapseColumn
     */
    public static function createGenericColumn(string $columnName): ColumnInterface
    {
        $definition = new Synapse(
            Synapse::TYPE_NVARCHAR,
            [
                'length' => '4000', // should be changed to max in future
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
     * @return Synapse
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }
}
