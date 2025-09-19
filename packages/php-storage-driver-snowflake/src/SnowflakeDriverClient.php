<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Contract\Driver\ClientInterface;
use Keboola\StorageDriver\Snowflake\Handler\HandlerFactory;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class SnowflakeDriverClient implements ClientInterface
{
    protected LoggerInterface $logger;

    public function __construct(
        LoggerInterface|null $logger = null,
    ) {
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
    }

    /**
     * @param string[] $features
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        $handler = HandlerFactory::create($command);

        $handledResponse = $handler(
            $credentials,
            $command,
            $features,
            $runtimeOptions,
        );

        $handler->setLogger($this->logger);

        $response = new DriverResponse();
        if ($handledResponse !== null) {
            $any = new Any();
            $any->pack($handledResponse);
            $response->setCommandResponse($any);
        }

        return $response;
    }
}
