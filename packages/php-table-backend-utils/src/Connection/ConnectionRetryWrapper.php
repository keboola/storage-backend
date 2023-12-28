<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Retry\RetryProxyInterface;

final class ConnectionRetryWrapper implements Connection
{
    private Connection $connection;

    private RetryProxyInterface $retryProxy;

    public function __construct(
        Connection $connection,
        RetryProxyInterface $retryProxy,
    ) {
        $this->connection = $connection;
        $this->retryProxy = $retryProxy;
    }

    public function prepare(string $sql): Statement
    {
        return $this->retryProxy->call(fn() => $this->connection->prepare($sql));
    }

    public function query(string $sql): Result
    {
        return $this->retryProxy->call(fn() => $this->connection->query($sql));
    }

    /**
     * {@inheritDoc}
     */
    public function quote($value, $type = ParameterType::STRING)
    {
        return $this->retryProxy->call(fn() => $this->connection->quote($value, $type));
    }

    public function exec(string $sql): int
    {
        return $this->retryProxy->call(fn() => $this->connection->exec($sql));
    }

    /**
     * {@inheritDoc}
     */
    public function lastInsertId($name = null)
    {
        return $this->retryProxy->call(fn() => $this->connection->lastInsertId($name));
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction()
    {
        return $this->retryProxy->call(fn() => $this->connection->beginTransaction());
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        return $this->retryProxy->call(fn() => $this->connection->commit());
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack()
    {
        return $this->retryProxy->call(fn() => $this->connection->rollBack());
    }
}
