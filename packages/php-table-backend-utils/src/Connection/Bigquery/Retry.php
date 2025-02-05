<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use Closure;
use GuzzleHttp\Exception\RequestException;
use JsonException;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Throwable;

final class Retry
{
    private const RETRY_MISSING_CREATE_JOB = 'bigquery.jobs.create';
    private const RETRY_SERVICE_ACCOUNT_NOT_EXIST = 'IAM setPolicy failed for Dataset';
    private const RETRY_ON_REASON = [
        'rateLimitExceeded',
        'userRateLimitExceeded',
        'backendError',
        'jobRateLimitExceeded',
    ];
    private const ALWAYS_RETRY_STATUS_CODES = [429, 500, 503];

    /**
     * helper method to overcome some irregular behavior of google bigquery client
     */
    public static function getRestRetryFunction(LoggerInterface $logger, bool $includeUnauthorized = false): Closure
    {
        return static function () use ($logger, $includeUnauthorized): Closure {
            // BigQuery client sometimes calls directly restRetryFunction with exception as first argument
            // But in other cases it expects to return callable which accepts exception as first argument
            $argsNum = func_num_args();
            if ($argsNum === 2) {
                $ex = func_get_arg(0);
                if ($ex instanceof Throwable) {
                    return Retry::getRetryDecider($logger, $includeUnauthorized)($ex);
                }
            }
            return Retry::getRetryDecider($logger, $includeUnauthorized);
        };
    }

    /**
     * @param bool $includeUnauthorized default false, google cloud sometimes returns 401 even when credentials are
     *     correct, but it is bit tricky since in case of invalid credentials for real, it could cause long waiting
     *     loop
     */
    public static function getRetryDecider(LoggerInterface $logger, bool $includeUnauthorized = false): Closure
    {
        return static function (Throwable $ex) use ($logger, $includeUnauthorized): bool {
            $statusCode = $ex->getCode();

            $retryOnStatusCodes = self::ALWAYS_RETRY_STATUS_CODES;
            if ($includeUnauthorized) {
                $retryOnStatusCodes[] = 401;
            }
            if (in_array($statusCode, $retryOnStatusCodes)) {
                Retry::logRetry($statusCode, [], $logger);
                return true;
            }
            if ($statusCode >= 200 && $statusCode < 300) {
                return false;
            }

            $message = $ex->getMessage();
            if ($ex instanceof RequestException && $ex->hasResponse()) {
                $message = (string) $ex->getResponse()?->getBody();
            }
            if (str_contains($message, self::RETRY_SERVICE_ACCOUNT_NOT_EXIST)) {
                Retry::logRetry($statusCode, [$message], $logger);
                return true;
            }
            if (str_contains($message, self::RETRY_MISSING_CREATE_JOB)) {
                Retry::logRetry($statusCode, $message, $logger);
                return true;
            }

            try {
                $message = json_decode($message, true, 512, JSON_THROW_ON_ERROR);
                assert(is_array($message));
            } catch (JsonException) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!array_key_exists('error', $message)) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!array_key_exists('errors', $message['error'])) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            if (!is_array($message['error']['errors'])) {
                Retry::logNotRetry($statusCode, $message, $logger);
                return false;
            }

            foreach ($message['error']['errors'] as $error) {
                if (array_key_exists('reason', $error) && in_array($error['reason'], self::RETRY_ON_REASON, false)) {
                    Retry::logRetry($statusCode, $message, $logger);
                    return true;
                }
            }

            Retry::logNotRetry($statusCode, $message, $logger);

            return false;
        };
    }

    /**
     * @param array<mixed> $message
     * @throws JsonException
     */
    private static function logRetry(int $statusCode, array|string $message, LoggerInterface $logger): void
    {
        if (is_array($message)) {
            $message = json_encode($message, JSON_THROW_ON_ERROR);
        }

        $logger->log(
            LogLevel::INFO,
            sprintf(
                'Retrying [%s] request with exception::%s',
                $statusCode,
                $message,
            ),
        );
    }

    /**
     * @param array<mixed> $message
     * @throws JsonException
     */
    private static function logNotRetry(int $statusCode, string|array $message, LoggerInterface $logger): void
    {
        if (is_array($message)) {
            $message = json_encode($message, JSON_THROW_ON_ERROR);
        }
        $logger->log(
            LogLevel::INFO,
            sprintf(
                'Not retrying [%s] request with exception::%s',
                $statusCode,
                $message,
            ),
        );
    }
}
