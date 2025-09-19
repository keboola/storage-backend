<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler;

use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

abstract class BaseHandler implements DriverCommandHandlerInterface
{
    protected LoggerInterface $logger;

    public function __construct(
        LoggerInterface|null $logger = null,
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    public function setLogger(LoggerInterface $logger): self
    {
        $this->logger = $logger;
        return $this;
    }
}
