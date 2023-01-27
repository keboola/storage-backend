<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Exception;

use Doctrine\DBAL\Driver\AbstractException;
use Throwable;
use function assert;

class DriverException extends AbstractException
{
    /**
     * @param resource $resource
     */
    public static function newFromHandle($resource): self
    {
        $errorCode = odbc_error($resource);
        assert($errorCode !== false);
        $errorMsg = odbc_errormsg($resource);

        return new self($errorMsg, null, (int) $errorCode);
    }

    public static function newConnectionFailure(string $message, int $code, ?Throwable $previous): self
    {
        return new self($message, null, $code, $previous);
    }
}
