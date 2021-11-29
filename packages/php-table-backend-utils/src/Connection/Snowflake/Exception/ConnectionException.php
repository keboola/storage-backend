<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake\Exception;

use Doctrine\DBAL\Exception\ConnectionException as DBAConnectionException;
use Doctrine\DBAL\Exception\RetryableException;

final class ConnectionException extends DBAConnectionException implements RetryableException
{
}
