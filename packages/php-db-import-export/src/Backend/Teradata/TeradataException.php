<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata;

use Doctrine\DBAL\Exception as DBALException;
use Keboola\Db\Import\Exception;
use Throwable;

class TeradataException extends Exception
{

    public static function covertException(DBALException $e): Throwable
    {
        // TODO
        return $e;
    }
}
