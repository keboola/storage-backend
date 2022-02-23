<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

use Google\Protobuf\Internal\Message;

interface DriverCommandHandlerInterface
{
    /**
     * @param string[] $features
     * @return mixed
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features
    );
}
