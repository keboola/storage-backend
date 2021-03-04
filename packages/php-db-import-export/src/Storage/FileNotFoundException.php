<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\Db\Import\Exception;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Throwable;

class FileNotFoundException extends Exception
{
    public function __construct(string $message = '', ?Throwable $previous = null)
    {
        parent::__construct('Load error: ' . $message, Exception::MANDATORY_FILE_NOT_FOUND, $previous);
    }

    public static function createFromFileNotFoundException(
        \Keboola\FileStorage\FileNotFoundException $e
    ): FileNotFoundException {
        return new self($e->getMessage(), $e);
    }

    public static function createFromServiceException(ServiceException $e): FileNotFoundException
    {
        return new self($e->getErrorText(), $e);
    }
}
