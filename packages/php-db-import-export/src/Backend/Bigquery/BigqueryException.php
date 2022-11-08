<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Google\Cloud\BigQuery\Exception\JobException;
use Keboola\Db\Import\Exception;
use Throwable;

class BigqueryException extends Exception
{
    public static function covertException(JobException $e): Throwable
    {
        // TODO
        return $e;
    }
}
