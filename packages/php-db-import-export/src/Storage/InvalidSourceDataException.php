<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Db\Import\Exception;
use Throwable;

class InvalidSourceDataException extends Exception
{
    public function __construct(string $message = '', ?Throwable $previous = null)
    {
        parent::__construct('Load error: ' . $message, Exception::INVALID_SOURCE_DATA, $previous);
    }

    public static function createFromDbalException(DBALException $e, string $message): InvalidSourceDataException
    {
        return new self($message, $e);
    }
}
