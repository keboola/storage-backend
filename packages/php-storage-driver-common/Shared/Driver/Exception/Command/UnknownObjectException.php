<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Driver\Exception\Command;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;

final class UnknownObjectException extends Exception implements
    ApplicationExceptionInterface,
    NonRetryableExceptionInterface
{
    public function __construct(string $objectName)
    {
        parent::__construct(sprintf(
            'Unknown object "%s".',
            $objectName
        ));
    }
}
