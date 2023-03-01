<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command\Import;

use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

/**
 * Import exception for invalid input which cannot be imported and is not retryable
 */
class ImportValidationException extends Exception implements
    NonRetryableExceptionInterface
{
    public function __construct(string $message, int $code = self::ERR_VALIDATION)
    {
        parent::__construct($message, $code);
    }
}
