<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Contract\Driver\Exception;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use Keboola\CommonExceptions\ExceptionWithContextInterface;

interface ExceptionInterface extends ApplicationExceptionInterface, ExceptionWithContextInterface
{
    public const ERR_UNKNOWN = 0;

    // Driver implementation exceptions 900-
    public const ERR_DRIVER_COMMAND_NOT_SUPPORTED = 900;

    // Connection related exception codes 1000-1099
    public const ERR_TIMEOUT = 1000;
    public const ERR_CONNECTION = 1001;
    public const ERR_AUTH = 1002;

    // Objects related exception codes 2xxx
    public const ERR_NOT_FOUND = 2000;
    public const ERR_UNKNOWN_OBJECT = 2001;
    public const ERR_DATABASE_NOT_FOUND = 2002;
    public const ERR_SCHEMA_NOT_FOUND = 2003;
    public const ERR_TABLE_NOT_FOUND = 2004;
    public const ERR_COLUMN_NOT_FOUND = 2005;

    // Validation exception codes 3xxx
    public const ERR_VALIDATION = 3000;

    // Import exceptions 4xxx
    public const ERR_UNKNOWN_IMPORT = 4000;
    public const ERR_COLUMNS_COUNT_NOT_MATCH = 4100;
    public const ERR_INVALID_COLUMN_NAME = 4101;
    public const ERR_DUPLICATE_COLUMN_NAMES = 4102;
    public const ERR_NO_COLUMNS = 4103;
    public const ERR_MANDATORY_INVALID_SOURCE_DATA = 4200;
    public const ERR_MANDATORY_FILE_NOT_FOUND = 4201;
    public const ERR_DATA_TYPE_MISMATCH = 4202;
    public const ERR_INVALID_FILE_PARAMS = 4203;

    // Export exceptions 5xxx
    public const ERR_UNKNOWN_EXPORT = 5000;

    // SQL exceptions 6xxx
    public const ERR_UNKNOWN_SQL = 6000;
    public const ERR_INVALID_SQL = 6001;
    public const ERR_QUERY_TIMEOUT = 6002;
    public const ERR_ROW_SIZE_TOO_LARGE = 6003;
    public const ERR_VALUE_CONVERSION = 6004;

    public function isRetryable(): bool;
}
