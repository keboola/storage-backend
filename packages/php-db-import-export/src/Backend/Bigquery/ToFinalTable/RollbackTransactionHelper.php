<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable;

use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * Helper to rollback transaction with retries and exponential backoff
 * when some query run fails on some unexpected state and job did not finished
 * running rollback immediately can fail because another job is still running
 * we want to retry in this case to increase probability that the blocking job will finish
 */
class RollbackTransactionHelper
{
    public static function rollbackTransaction(
        BigQueryClient $bqClient,
        Session $session,
        SqlBuilder $sqlBuilder,
    ): void {
        $proxy = new RetryProxy(
            new SimpleRetryPolicy(10),
            // Set retry with exponential backoff, max 30s
            // this 10 attempts will take roughly something above 1 minute
            // usually rollback is blocked by truncate table command
            new ExponentialRandomBackOffPolicy(500, 1.5, 30000),
        );
        $proxy->call(fn() => $bqClient->runQuery($bqClient->query(
            $sqlBuilder->getRollbackTransaction(),
            $session->getAsQueryOptions(),
        )));
    }
}
