<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Collection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use function Keboola\Utils\returnBytes;

final class TeradataTableReflection implements TableReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $dbName;

    /** @var string */
    private $tableName;

    /** @var bool */
    private $isTemporary;

    public function __construct(Connection $connection, string $dbName, string $tableName)
    {
        // TODO detect temp tables
        $this->isTemporary = false;

        $this->tableName = $tableName;
        $this->dbName = $dbName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'HELP TABLE %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->dbName),
                TeradataQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        return Collection::extractByKey($columns, 'Column Name');
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
//        TODO
        return new ColumnCollection([]);
    }

    public function getRowsCount(): int
    {


        return (int) 0;
    }

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array
    {
        return [];
    }

    public function getTableStats(): TableStatsInterface
    {
//        TODO
        return new TableStats(1, 1);
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }

    /**
     * @return array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array
    {
//        TODO

        return [];
    }


    public function getTableDefinition(): TableDefinitionInterface
    {
//        TODO
//        return new TeradataTableDefinition(
//            $this->dbName,
//            $this->tableName,
//            $this->isTemporary(),
//            $this->getColumnsDefinitions(),
//            $this->getPrimaryKeysNames(),
//            new TableDistributionDefinition(
//                $this->getTableDistribution(),
//                $this->getTableDistributionColumnsNames()
//            ),
//            new TableIndexDefinition(
//                $this->getTableIndexType(),
//                []
//            )
//        );
    }
}
