<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver;

use Google\Protobuf\Internal\Message;

interface ClientInterface
{
    /**
     * @param string[] $features
     * @return mixed
     */
    public function runCommand(
        Message $credentials,
        Message $command,
        array $features
    );
}
