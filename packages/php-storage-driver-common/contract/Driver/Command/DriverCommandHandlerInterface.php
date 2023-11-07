<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Command;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\DriverResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;

interface DriverCommandHandlerInterface
{
    /**
     * @param string[] $features
     * @param RuntimeOptions $runtimeOptions
     * @return DriverResponse|Message|null
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message;
}
