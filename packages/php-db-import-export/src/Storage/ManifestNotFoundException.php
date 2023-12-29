<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\Db\Import\Exception;
use Throwable;

class ManifestNotFoundException extends Exception
{
    public function __construct(?Throwable $previous = null)
    {
        parent::__construct(
            'Load error: manifest file was not found.',
            Exception::MANDATORY_FILE_NOT_FOUND,
            $previous,
        );
    }
}
