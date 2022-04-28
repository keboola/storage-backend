<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    /** @var string[] */
    private $columnsNames;

    /** @var string[]|null */
    private $primaryKeysNames;

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $schema,
        string $tableName,
        array $columns = [],
        ?array $primaryKeysNames = null
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->columnsNames = $columns;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    public function getFromStatement(): string
    {
        $quotedColumns = array_map(function ($column) {
            return SynapseQuote::quoteSingleIdentifier($column);
        }, $this->getColumnsNames());

        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
        }

        return sprintf('SELECT %s FROM %s', $select, $this->getQuotedTableWithScheme());
    }

    public function getFromStatementForStaging(bool $castValues): string
    {
        $quotedColumns = array_map(
            static fn(string $column) => SynapseQuote::quoteSingleIdentifier($column),
            $this->getColumnsNames()
        );

        $castedColumns = [];
        if ($castValues === true) {
            $castedColumns = array_map(
                static fn(string $column): string => sprintf(
                    'CAST(%s as NVARCHAR(4000)) AS %s',
                    $column,
                    $column
                ),
                $quotedColumns
            );
        }

        $from = $this->getQuotedTableWithScheme();
        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
            if ($castValues === true) {
                $quotedColumns = array_map(
                    static fn(string $column) => sprintf('a.%s', $column),
                    $quotedColumns
                );
                $select = implode(', ', $quotedColumns);
                $from = sprintf(
                    '(SELECT %s FROM %s) AS a',
                    implode(', ', $castedColumns),
                    $from
                );
            }
        }
        return sprintf('SELECT %s FROM %s', $select, $from);
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            SynapseQuote::quoteSingleIdentifier($this->schema),
            SynapseQuote::quoteSingleIdentifier($this->tableName)
        );
    }

    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    public function getQueryBindings(): array
    {
        return [];
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
