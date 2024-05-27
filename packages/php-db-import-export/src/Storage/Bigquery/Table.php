<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Bigquery;

use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    private string $schema;

    private string $tableName;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames = null;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $schema,
        string $tableName,
        array $columnsNames = [],
        ?array $primaryKeysNames = null,
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    public function getFromStatement(): string
    {
        $quotedTable = BigqueryQuote::quoteSingleIdentifier($this->getTableName());
        $quotedColumns = array_map(
            static fn($column) => sprintf(
                '%s.%s',
                $quotedTable,
                BigqueryQuote::quoteSingleIdentifier($column),
            ),
            $this->getColumnsNames(),
        );

        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
        }

        return sprintf(
            'SELECT %s FROM %s.%s',
            $select,
            BigqueryQuote::quoteSingleIdentifier($this->getSchema()),
            $quotedTable,
        );
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    /** @return string[]|null */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /** @return array<mixed> */
    public function getQueryBindings(): array
    {
        return [];
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->schema),
            BigqueryQuote::quoteSingleIdentifier($this->tableName),
        );
    }

    public function getFromStatementWithStringCasting(): string
    {
        $quotedColumns = array_map(static function ($column) {
            return sprintf('CAST(%s AS STRING)', BigqueryQuote::quoteSingleIdentifier($column));
        }, $this->getColumnsNames());

        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
        }

        return sprintf(
            'SELECT %s FROM %s.%s',
            $select,
            BigqueryQuote::quoteSingleIdentifier($this->getSchema()),
            BigqueryQuote::quoteSingleIdentifier($this->getTableName()),
        );
    }
}
