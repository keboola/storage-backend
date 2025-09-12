<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver;

use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Common\LogMessage;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Shared\Driver\Logger\UserInMemoryLogger;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Base handler implementation
 * provides internal logger and user logger
 * Internal logger is meant for debugging in tests
 *  - handles sets internal messages
 *  - tests can check these messages
 *  - messages are also dumped to stdout in tests
 * User logger is meant for messages that will be returned to the user
 *  - handler can set messages for the user
 *  - response can optionally set these messages to be returned
 */
abstract class BaseHandler implements DriverCommandHandlerInterface
{
    protected LoggerInterface $internalLogger;

    protected UserInMemoryLogger $userLogger;

    public function __construct(
        ?LoggerInterface $internalLogger = null,
    ) {
        if ($internalLogger === null) {
            $this->internalLogger = new NullLogger();
        } else {
            $this->internalLogger = $internalLogger;
        }
        $this->userLogger = new UserInMemoryLogger();
    }

    public function setInternalLogger(LoggerInterface $logger): self
    {
        $this->internalLogger = $logger;
        return $this;
    }

    /**
     * @return RepeatedField<LogMessage>
     */
    public function getMessages(): RepeatedField
    {
        return $this->userLogger->getLogs();
    }
}
