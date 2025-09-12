<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Contract\Driver\Command\DriverCommandHandlerInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateDevBranchHandler;
use Keboola\StorageDriver\Snowflake\Handler\Table\ProfileTableHandler;

final class HandlerFactory
{
    public static function create(Message $command): DriverCommandHandlerInterface
    {
        return match ($command::class) {
            CreateProfileTableCommand::class => new ProfileTableHandler(),
            CreateDevBranchCommand::class => new CreateDevBranchHandler(),
            default => throw new CommandNotSupportedException($command::class),
        };
    }
}
