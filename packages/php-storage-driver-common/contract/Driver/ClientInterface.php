<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;

interface ClientInterface
{
    /**
     * @param string[] $features
     * @param RuntimeOptions $runtimeOptions
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message;
}
