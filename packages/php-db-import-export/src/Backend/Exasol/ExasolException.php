<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;

class ExasolException extends Exception
{
    private const MANIFEST_ENTRY_NOT_FOUND = 'failed with error code=404';

    public static function covertException(\Doctrine\DBAL\Exception $e): \Throwable
    {
        if (strpos($e->getMessage(), self::MANIFEST_ENTRY_NOT_FOUND) !== false) {
            // file can not found during import

            // strip query from message, there are things like SAS tokens and internal table names
            preg_match('/^(An exception occurred.*?)(Following error.*)=?.\(Session.*/ms', $e->getMessage(), $matches);

            return FileNotFoundException::createFromDbalException($e, $matches[2]);
        }

        return $e;
    }
}
