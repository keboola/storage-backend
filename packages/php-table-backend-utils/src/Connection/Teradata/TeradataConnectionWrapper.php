<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\ParameterType;
use Exception;
use Keboola\TableBackendUtils\Connection\Exception\DriverException;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Throwable;

class TeradataConnectionWrapper implements Connection
{
    /** @var resource */
    private $conn;

    public function __construct(
        string $dsn,
        string $user,
        string $password,
    ) {
        try {
            $handle = odbc_connect($dsn, $user, $password);
            assert($handle !== false);
            $this->conn = $handle;
        } catch (Throwable $e) {
            throw DriverException::newConnectionFailure($e->getMessage(), (int) $e->getCode(), $e->getPrevious());
        }
    }

    public function query(string $sql): Result
    {
        $stmt = $this->prepare($sql);
        return $stmt->execute();
    }

    public function prepare(string $sql): TeradataStatement
    {
        return new TeradataStatement($this->conn, $sql);
    }

    /**
     * @inheritDoc
     */
    public function quote($value, $type = ParameterType::STRING): string
    {
        assert(is_string($value));
        return TeradataQuote::quote($value);
    }

    public function exec(string $sql): int
    {
        $stmt = $this->prepare($sql);
        $result = $stmt->execute();

        return $result->rowCount();
    }

    /**
     * @inheritDoc
     */
    public function lastInsertId($name = null)
    {
        // TODO: Implement lastInsertId() method.
        throw new Exception('method is not implemented yet');
    }

    public function beginTransaction(): bool
    {
        if ($this->inTransaction()) {
            throw new DriverException('There is already an active transaction');
        }
        return (bool) odbc_autocommit($this->conn, false);
    }


    private function inTransaction(): bool
    {
        return !odbc_autocommit($this->conn);
    }

    public function commit(): bool
    {
        if (!$this->inTransaction()) {
            throw new DriverException('There is no active transaction');
        }
        return odbc_commit($this->conn) && odbc_autocommit($this->conn, true);
    }

    public function rollBack(): bool
    {
        if (!$this->inTransaction()) {
            throw new DriverException('There is no active transaction');
        }
        return odbc_rollback($this->conn) && odbc_autocommit($this->conn, true);
    }

    public function errorCode(): ?string
    {
        return odbc_error($this->conn);
    }

    /**
     * @inheritDoc
     * @return array{code:string, message:string}
     */
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
