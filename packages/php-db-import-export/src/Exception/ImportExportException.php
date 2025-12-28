<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Exception;

use Keboola\Db\Import\Exception as LegacyException;

/**
 * Import export lib basic exception
 * this exception should be appropriately converted to user and internal exceptions
 *
 * Each backend should have own code prefix:
 * Snowflake - 11xx
 * Redshift - 12xx
 *
 * Common exceptions have code <1000 (Keboola\Db\Import\Exception)
 *
 */
class ImportExportException extends LegacyException
{
}
