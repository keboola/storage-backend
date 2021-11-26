<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\PDO\Exception;
use Doctrine\DBAL\ParameterType;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeConnection implements Connection
{
    /** @var resource */
    private $conn;

    /**
     * @param mixed[]|null $options
     */
    public function __construct(
        string $dsn,
        string $user,
        string $password,
        ?array $options
    ) {
        try {
            $this->conn = odbc_connect($dsn, $user, $password);

            if (isset($options['runId'])) {
                $queryTag = [
                    'runId' => $options['runId'],
                ];
                $this->query("ALTER SESSION SET QUERY_TAG='" . json_encode($queryTag) . "';");
            }
        } catch (\Throwable $e) {
            throw Exception::new(new \PDOException($e->getMessage(), $e->getCode(), $e->getPrevious()));
        }
    }

    public function query(string $sql): \Doctrine\DBAL\Driver\Result
    {
        $stmt = $this->prepare($sql);
        return $stmt->execute();
    }

    public function prepare($sql): SnowflakeStatement
    {
        return new SnowflakeStatement($this->conn, $sql);
    }

    public function quote($value, $type = ParameterType::STRING)
    {
        return SnowflakeQuote::quote($value);
    }

    public function exec($sql): int
    {
        $stmt = $this->prepare($sql);
        $result = $stmt->execute();

        return $result->rowCount();
    }

    public function lastInsertId($name = null)
    {
        // TODO: Implement lastInsertId() method.
    }

    public function beginTransaction()
    {
        $this->checkTransactionStarted(false);
        return odbc_autocommit($this->conn, false);
    }

    private function checkTransactionStarted(bool $flag = true): void
    {
        if ($flag && !$this->inTransaction()) {
            throw new SnowflakeDriverException('Transaction was not started');
        }
        if (!$flag && $this->inTransaction()) {
            throw new SnowflakeDriverException('Transaction was already started');
        }
    }

    /**
     * @return bool
     */
    private function inTransaction(): bool
    {
        return !odbc_autocommit($this->conn);
    }

    public function commit(): bool
    {
        $this->checkTransactionStarted();

        return odbc_commit($this->conn) && odbc_autocommit($this->conn, true);
    }

    public function rollBack(): bool
    {
        $this->checkTransactionStarted();

        return odbc_rollback($this->conn) && odbc_autocommit($this->conn, true);
    }

    public function errorCode(): ?string
    {
        return odbc_error($this->conn);
    }

    public function errorInfo(): array
    {
        return [
            'code' => odbc_error($this->conn),
            'message' => odbc_errormsg($this->conn),
        ];
    }

    public function __destruct()
    {
        if (is_resource($this->conn)) {
            odbc_close($this->conn);
        }
    }
}
