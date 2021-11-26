<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Driver\OCI8\Exception\Error;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\ParameterType;

class SnowflakeStatement implements Statement
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
     * @var array
     */
    private $params = [];

    private string $query;

    /**
     * @param resource $dbh database handle
     * @param string $query
     * @param array $options
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
            throw new Exception(sprintf(
                '[%s] %s',
                odbc_errormsg($this->dbh),
                odbc_error($this->dbh)
            ));
        }

        return $stmt;
    }

    public function bindValue($param, $value, $type = ParameterType::STRING): bool
    {
        return $this->bindParam($param, $value, $type);
    }

    public function bindParam($param, &$variable, $type = ParameterType::STRING, $length = null): bool
    {
        $this->params[$param] = &$variable;
        return true;
    }

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

        $ret = odbc_execute($this->stmt, $this->repairBinding($this->params));
        if (! $ret) {
            throw Error::new($this->stmt);
        }

        return new Result($this->stmt);
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     *
     * @param array $bind
     * @return array
     */
    private function repairBinding(array $bind): array
    {
        return array_map(function ($value) {
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            }

            return $value;
        }, $bind);
    }
}
