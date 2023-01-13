<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command\Import;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Throwable;

/**
 * General import exception which can be retied
 */
class ImportException extends Exception
{
    public function __construct(string $message, int $code = self::ERR_UNKNOWN_IMPORT, ?Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }

    public static function createTimeout(string $message, ?Throwable $previous = null): self
    {
        return new self($message, self::ERR_QUERY_TIMEOUT, $previous);
    }
}
