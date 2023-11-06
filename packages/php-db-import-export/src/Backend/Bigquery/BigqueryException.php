<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Exception;
use Throwable;

class BigqueryException extends Exception
{
    public static function covertException(JobException|ServiceException $e): Throwable
    {
        // TODO
        return $e;
    }
}
