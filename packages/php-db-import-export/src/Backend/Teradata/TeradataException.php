<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata;

use Keboola\Db\Import\Exception;

class TeradataException extends Exception
{

    public static function covertException(\Doctrine\DBAL\Exception $e): \Throwable
    {
        return $e;
    }
}
