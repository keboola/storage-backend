<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Doctrine\DBAL\Types\Type;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class SelectSource implements SourceInterface, SqlSourceInterface
{
    private string $query;

    /** @var array<mixed> */
    private array $queryBindings;

    /** @var array<int|string, Type|int|string|null> */
    private array $dataTypes;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames = null;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     * @param array<int|string, Type|int|string|null> $dataTypes
     * @param array<mixed>|null $queryBindings
     */
    public function __construct(
        string $query,
        array $queryBindings = [],
        array $dataTypes = [],
        array $columnsNames = [],
        ?array $primaryKeysNames = null
    ) {
        $this->query = $query;
        $this->queryBindings = $queryBindings;
        $this->dataTypes = $dataTypes;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    /** @return string[] */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    /** @return array<int|string, Type|int|string|null> */
    public function getDataTypes(): array
    {
        return $this->dataTypes;
    }

    public function getFromStatement(): string
    {
        return sprintf('%s', $this->getQuery());
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    /** @return string[]|null */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /** @return array<mixed> */
    public function getQueryBindings(): array
    {
        return $this->queryBindings;
    }

    public function getFromStatementWithStringCasting(): string
    {
        return $this->getFromStatement();
    }
}
