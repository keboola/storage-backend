<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\Helper;

use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;

trait DropTableTrait
{
    public function dropIfExists($dbName, $tableName): void
    {
        if ($this->tableExists($dbName, $tableName)
        ) {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableUnsafe($dbName, $tableName)
            );
        }
    }

    protected function tableExists(string $dbName, string $tableName): bool
    {
        $data = $this->connection->fetchOne((new SqlBuilder())->getTableExistsCommand($dbName, $tableName));
        return ((int) $data) > 0;
    }
}
