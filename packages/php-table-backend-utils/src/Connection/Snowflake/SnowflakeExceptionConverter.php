<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Query;
use Keboola\TableBackendUtils\Connection\Exception\ConnectionException;
use Keboola\TableBackendUtils\Connection\Exception\DriverException as SnowflakeDriverException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\AccountDecommissionedException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\CannotAccessObjectException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\StringTooLongException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\WarehouseTimeoutReached;

class SnowflakeExceptionConverter implements ExceptionConverter
{
    private const CODE_CANCELED = 57014;
    private const CODE_STRING_TRUNCATE = 22000;

    public function convert(Exception $exception, ?Query $query): DriverException
    {
        $pattern = '/Incorrect username or password was specified/';
        $matches = null;
        if (preg_match($pattern, $exception->getMessage(), $matches)) {
            return new ConnectionException(
                new SnowflakeDriverException(
                    'Incorrect username or password was specified.',
                    null,
                    $exception->getCode(),
                    $exception,
                ),
                $query,
            );
        }

        $pattern = '/This account has been marked for decommission/';
        if (preg_match($pattern, $exception->getMessage())) {
            return new AccountDecommissionedException(
                new SnowflakeDriverException(
                    'Snowflake account is marked for decommission. ' .
                    'Please contact your administrator or update the component configuration to use an active account.',
                    null,
                    $exception->getCode(),
                    $exception,
                ),
                $query,
            );
        }

        $pattern = "/String \'([^\']*)\' is too long and would be truncated/";
        $matches = null;
        if ($exception->getCode() === self::CODE_STRING_TRUNCATE
            && preg_match($pattern, $exception->getMessage(), $matches)
        ) {
            array_shift($matches); // remove the whole string from matches
            return new StringTooLongException(
                new SnowflakeDriverException(
                    vsprintf(
                        "String '%s' cannot be inserted because it's bigger than column size",
                        $matches,
                    ),
                    null,
                    $exception->getCode(),
                    $exception,
                ),
                $query,
            );
        }

        $pattern = '/Statement reached its statement or warehouse timeout of ([0-9]+) second.*/';
        $matches = null;
        if ($exception->getCode() === self::CODE_CANCELED && preg_match($pattern, $exception->getMessage(), $matches)) {
            array_shift($matches); // remove the whole string from matches
            return new WarehouseTimeoutReached(
                new SnowflakeDriverException(
                    vsprintf(
                        'Query reached its timeout %d second(s)',
                        $matches,
                    ),
                    null,
                    $exception->getCode(),
                    $exception,
                ),
                $query,
            );
        }

        if ($query !== null && strpos($exception->getMessage(), 'Object does not exist')) {
            return new CannotAccessObjectException(
                new SnowflakeDriverException(
                    sprintf('Cannot access object or it does not exist. Executing query "%s"', $query->getSQL()),
                    null,
                    $exception->getCode(),
                    $exception,
                ),
                $query,
            );
        }

        return new DriverException($exception, $query);
    }
}
