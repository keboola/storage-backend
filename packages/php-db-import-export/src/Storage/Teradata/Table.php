<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Teradata;

use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

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
        $select = '*';
        $colums = $this->getColumnsNames();
        if ($colums !== []) {
            $quotedColumns = array_map(static function ($column) {
                // trim because implicit casting adds right padding spaces
                // value 10.5 as DECIMAL(8,1) implicitly casted to varchar would be then "      10.5"
                return sprintf("TRIM(%s)", TeradataQuote::quoteSingleIdentifier($column));
            }, $colums);
            $select = implode(', ', $quotedColumns);
        }

        return sprintf('SELECT %s FROM %s', $select, $this->getQuotedTableWithScheme());
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
            TeradataQuote::quoteSingleIdentifier($this->getSchema()),
            TeradataQuote::quoteSingleIdentifier($this->getTableName())
        );
    }

    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /**
     * @return string[]
     */
    public function getQueryBindings(): array
    {
        // TODO
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
