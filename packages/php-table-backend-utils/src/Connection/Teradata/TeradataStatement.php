<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\DriverException;

class TeradataStatement implements Statement
{
    /**
     * @var resource
     */
    private $dbh;

    /**
     * @var resource
     */
    private $stmt;

    /**
     * @var array<mixed>
     */
    private array $params = [];

    private string $query;

    /**
     * @param resource $dbh database handle
     * @param string $query
     */
    public function __construct($dbh, string $query)
    {
        $this->dbh = $dbh;
        $this->query = $query;
        $this->stmt = $this->prepare();
    }

    /**
     * @return resource
     */
    private function prepare()
    {
        $stmt = @odbc_prepare($this->dbh, $this->query);
        if (!$stmt) {
            throw DriverException::newFromHandle($this->dbh);
        }
        return $stmt;
    }

    /**
     * @inheritDoc
     */
    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        return $this->bindParam($param, $value, $type);
    }

    /**
     * @inheritDoc
     */
    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $this->params[$param] = &$variable;
        return true;
    }

    /**
     * @inheritDoc
     */
    public function execute($params = null): Result
    {
        if (!empty($params) && is_array($params)) {
            foreach ($params as $pos => $value) {
                if (is_int($pos)) {
                    $pos += 1;
                }
                $this->bindValue($pos, $value);
            }
        }

        try {
            odbc_execute($this->stmt, $this->repairBinding($this->params));
        } catch (\Throwable $e) {
            throw DriverException::newFromHandle($this->dbh);
        }

        return new Result($this->stmt);
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     *
     * @param array<mixed> $bind
     * @return array<mixed>
     */
    private function repairBinding(array $bind): array
    {
        return array_map(static function ($value) {
            if (!is_string($value)) {
                return $value;
            }
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            }

            return $value;
        }, $bind);
    }
}
