<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;

class TableBackendUtilException extends RuntimeException implements ApplicationExceptionInterface
{
}
