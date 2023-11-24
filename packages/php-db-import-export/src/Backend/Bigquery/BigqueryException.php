<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery;

use Google\Cloud\BigQuery\Exception\JobException;
use Google\Cloud\Core\Exception\ServiceException;
use Keboola\Db\Import\Exception;
use Throwable;

class BigqueryException extends Exception
{
    const MAX_MESSAGES_IN_ERROR_MESSAGE = 10;

    public static function covertException(JobException|ServiceException $e): Throwable
    {
        if ($e instanceof ServiceException) {
            if (preg_match('/.*Required field .+ cannot be null.*/m', $e->getMessage(), $output_array) === 1) {
                return new BigqueryInputDataException($e->getMessage());
            }
            return new self($e->getMessage());
        }
        return $e;
    }

    public static function createExceptionFromJobResult(array $jobInfo): Throwable
    {
        $errorMessage = $jobInfo['status']['errorResult']['message'] ?? 'Unknown error';

        $pattern = '/Error while reading data, error message: CSV processing encountered too many errors, giving up. Rows: [0-9]+; errors: [0-9]+; max bad: [0-9]+; error percent: [0-9]+/';
        $jobErrors = $jobInfo['status']['errors'] ?? [];

        // detecting missing required column. Record with `Required column value is missing` substring contains
        // much better information for enduser
        foreach ($jobErrors as $error) {
            if (str_contains($error['message'], 'Required column value is missing')) {
                $errorMessage = $error['message'];
                return new BigqueryInputDataException($errorMessage);
            }
        }

        $isMultipleErrors = preg_match($pattern, $errorMessage, $matches);
        $filteredJobErrors = array_filter($jobErrors, function ($error) use ($jobInfo) {
            // the errorResult is the first in list of errors as well
            return $error['message'] !== $jobInfo['status']['errorResult']['message'];
        });
        $countOfErrors = count($filteredJobErrors);
        if ($isMultipleErrors) {
            // filter parsing errors
            $parsingErrors = array_filter($filteredJobErrors, function ($error) {
                return self::isUserError($error['message'], $error['reason']);
            });
            $areExtraErrors = count($parsingErrors) !== $countOfErrors;
            return new BigqueryInputDataException(self::getErrorMessageForErrorList($parsingErrors, $areExtraErrors, $jobInfo['jobReference']['jobId']));

            if (count($filteredJobErrors) > 0 && count($filteredJobErrors) < 10) {
                // if there is reasonable number of errors, we can output them all
                $errors = implode(PHP_EOL, array_map(function ($error) {
                    return $error['message'];
                }, $filteredJobErrors));
                return new BigqueryInputDataException('CSV processing failed:' . PHP_EOL . $errors);
            }

            if (count($filteredJobErrors) > 10) {
                $filteredJobErrors = array_slice($filteredJobErrors, 0, 10);
                // if there is too many errors, we can output only first 10 with link to further details
                $tooManyErrorsMessage = sprintf(
                    'CSV processing failed with too many errors (for details see job detail %s):',
                    $jobInfo['selfLink'],
                );
                $errors = implode(PHP_EOL, array_map(function ($error) {
                    return $error['message'];
                }, $filteredJobErrors));
                return new BigqueryInputDataException($tooManyErrorsMessage . PHP_EOL . $errors);
            }
            // else there are no further errors, so it's not the expected case, let it fall through
        }

        return new self($errorMessage);
    }

    private static function isUserError(string $message, string $reason): bool
    {
        if (str_contains($message, 'Required column value is missing')) {
            return true;
        }
        if (str_contains($message, 'Could not parse')) {
            return true;
        }
        return false;
    }

    private static function getErrorMessageForErrorList(array $parsingErrors, bool $areExtraErrors, string $jobId)
    {
        $count = count($parsingErrors);
        if ($count > self::MAX_MESSAGES_IN_ERROR_MESSAGE) {
            return sprintf('There were too many errors during the import. For more information check job "%s" in Google Cloud Console.', $jobId);
        }

        $message = implode(PHP_EOL, array_map(function ($error) {
            return $error['message'];
        }, $parsingErrors));
        if ($areExtraErrors) {
            $message .= PHP_EOL . sprintf('There were additional errors during the import. For more information check job "%s" in Google Cloud Console.', $jobId);
        }

        return $message;
    }
}
