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
        if ($e instanceof ServiceException) {
            if (str_contains($e->getMessage(), 'Required column value is missing')) {
                return new BigqueryInputDataException($e->getMessage());
            }
            return new self($e->getMessage());
        }
        return $e;
    }

    public static function createExceptionFromJobResult(array $jobInfo): Throwable
    {
        $errorMessage = $jobInfo['status']['errorResult']['message'] ?? 'Unknown error';

        // detecting missing required column. Record with `Required column value is missing` substring contains
        // much better information for enduser
        foreach ($jobInfo['status']['errors'] ?? [] as $error) {
            if (str_contains($error['message'], 'Required column value is missing')) {
                $errorMessage = $error['message'];
                return new BigqueryInputDataException($errorMessage);
            }
        }

        return new self($errorMessage);
    }
}


