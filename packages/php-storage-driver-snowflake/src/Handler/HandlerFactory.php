<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\DropDevBranchCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Snowflake\Handler\Bucket\CreateBucketHandler;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateDevBranchHandler;
use Keboola\StorageDriver\Snowflake\Handler\Project\DropDevBranchHandler;
use Keboola\StorageDriver\Snowflake\Handler\Table\ProfileTableHandler;
use Psr\Log\LoggerInterface;

final class HandlerFactory
{
    public static function create(
        Message $command,
        LoggerInterface $internalLogger,
    ): DriverCommandHandlerInterface {
        $handler = match ($command::class) {
            CreateProfileTableCommand::class => new ProfileTableHandler(),
            CreateBucketCommand::class => new CreateBucketHandler(),
            CreateDevBranchCommand::class => new CreateDevBranchHandler(),
            DropDevBranchCommand::class => new DropDevBranchHandler(),
            default => throw new CommandNotSupportedException($command::class),
        };

        $handler->setInternalLogger($internalLogger);

        return $handler;
    }
}
