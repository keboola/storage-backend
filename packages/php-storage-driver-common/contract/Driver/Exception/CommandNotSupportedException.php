<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;

final class CommandNotSupportedException extends Exception implements
    ApplicationExceptionInterface,
    NonRetryableExceptionInterface
{
    public function __construct(string $command)
    {
        parent::__construct(sprintf(
            'Command "%s" not supported.',
            $command
        ));
    }
}
