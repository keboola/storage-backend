<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result as ResultInterface;

class Result implements ResultInterface
{
    /** @var resource */
    private $statement;

    /**
     * @param resource $statement
     */
    public function __construct($statement)
    {
        $this->statement = $statement;
    }

    public function fetchNumeric()
    {
        return $this->fetch(\PDO::FETCH_NUM);
    }

    public function fetchAssociative()
    {
        return $this->fetch(\PDO::FETCH_ASSOC);
    }

    public function fetchOne()
    {
        return FetchUtils::fetchOne($this);
    }

    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    public function rowCount(): int
    {
        return odbc_num_rows($this->statement);
    }

    public function columnCount(): int
    {
        return odbc_num_fields($this->statement);
    }

    public function free(): void
    {
        odbc_free_result($this->statement);
    }


    public function fetch($fetchMode)
    {
        if (!odbc_fetch_row($this->statement)) {
            return false;
        }
        $numFields = odbc_num_fields($this->statement);
        $row = [];
        switch ($fetchMode) {
            case \PDO::FETCH_ASSOC:
                for ($i = 1; $i <= $numFields; $i++) {
                    $row[odbc_field_name($this->statement, $i)] = odbc_result($this->statement, $i);
                }
                break;

            case \PDO::FETCH_NUM:
                for ($i = 1; $i <= $numFields; $i++) {
                    $row[] = odbc_result($this->statement, $i);
                }
                break;
            default:
                throw new \InvalidArgumentException(sprintf('Unsupported fetch mode "%s"', $fetchMode));
        }

        return $row;
    }
}
