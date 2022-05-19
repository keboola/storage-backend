<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol;

use Doctrine\DBAL\Exception as DBALAlias;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\FileNotFoundException;
use Throwable;

class ExasolException extends Exception
{
    private const MANIFEST_ENTRY_NOT_FOUND = 'failed with error code=404';
    private const CONSTRAINT_VIOLATION_NOT_NULL = 'constraint violation - not null';

    public static function covertException(DBALAlias $e): Throwable
    {
        if (strpos($e->getMessage(), self::MANIFEST_ENTRY_NOT_FOUND) !== false) {
            // file can not found during import

            // strip query from message, there are things like SAS tokens and internal table names
            preg_match('/^(An exception occurred.*?)(Following error.*)=?.\(Session.*/ms', $e->getMessage(), $matches);

            return FileNotFoundException::createFromDbalException($e, $matches[2]);
        }

        if (strpos($e->getMessage(), self::CONSTRAINT_VIOLATION_NOT_NULL) !== false) {
            // strip query from message, there are things like internal table names
            preg_match(
                '/^(An exception occurred.*?)(constraint violation - not null.*)=?.in table.*/ms',
                $e->getMessage(),
                $matches
            );

            return new Exception(
                'Load error: ' . ucfirst($matches[2]) . ').',
                Exception::UNKNOWN_ERROR
            );
        }

        return $e;
    }
}
