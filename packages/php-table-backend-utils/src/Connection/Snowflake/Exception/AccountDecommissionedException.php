<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake\Exception;

use Doctrine\DBAL\Exception\ServerException;

final class AccountDecommissionedException extends ServerException
{
}
