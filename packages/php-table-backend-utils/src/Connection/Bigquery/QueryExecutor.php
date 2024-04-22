<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Job;
use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\BigQuery\QueryResults;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\BackOff\ExponentialRandomBackOffPolicy;

class QueryExecutor
{
    private BackOffPolicyInterface $backOffPolicy;

    public function __construct(
        private readonly BigQueryClient $client,
        private readonly string $runId,
        BackOffPolicyInterface|null $backOffPolicy = null,
    ) {
        if ($backOffPolicy === null) {
            $this->backOffPolicy = new ExponentialRandomBackOffPolicy(
                ExponentialBackOffPolicy::DEFAULT_INITIAL_INTERVAL,
                1.8,
                10_000, // 10s
            );
        } else {
            $this->backOffPolicy = $backOffPolicy;
        }
    }

    /**
     * Replacement for BigQueryClient::runQuery with better pooling
     * With support for setting runId
     *
     * @param array<mixed> $options
     */
    public function runQuery(QueryJobConfiguration $query, array $options = []): QueryResults
    {
        if ($this->runId !== '') {
            $query = $query->labels(['run_id' => $this->runId]);
            assert($query instanceof QueryJobConfiguration);
        }
        $job = $this->client->startQuery($query, $options);

        $context = $this->backOffPolicy->start();
        do {
            $this->backOffPolicy->backOff($context);
            $job->reload();
        } while (!$job->isComplete());

        return $job->queryResults();
    }

    /**
     * Replacement for BigQueryClient::runJob with runId support
     *
     * @param array<mixed> $options
     */
    public function runJob(JobConfigurationInterface $config, array $options = []): Job
    {
        if ($this->runId !== '' && method_exists($config, 'labels')) {
            $config = $config->labels(['run_id' => $this->runId]);
        }
        return $this->client->runJob($config, $options);
    }
}
