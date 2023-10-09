<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

final class TooManyRequestsException extends Exception
{
    public const TOO_MANY_REQUESTS = 'Command did not succeeded due to too many requests.';

    public function __construct(string $message = self::TOO_MANY_REQUESTS, int $code = self::ERR_RESOURCE_FULL, ?Throwable $previous = null)
    {
        parent::__construct(
            $message,
            $code,
            $previous
        );
    }
}
