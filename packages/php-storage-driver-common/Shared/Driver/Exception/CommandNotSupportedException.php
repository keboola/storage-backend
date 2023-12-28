<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;

final class CommandNotSupportedException extends Exception implements
    NonRetryableExceptionInterface
{
    public function __construct(string $command)
    {
        parent::__construct(sprintf(
            'Command "%s" not supported.',
            $command,
        ));
    }
}
