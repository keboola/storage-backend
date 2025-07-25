<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Job;
use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryResults;
use InvalidArgumentException;
use Retry\BackOff\BackOffPolicyInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\BackOff\ExponentialRandomBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class BigQueryClientWrapper extends BigQueryClient
{
    private BackOffPolicyInterface $backOffPolicy;

    private readonly QueryTags $queryTags;
    /**
     * @inheritdoc
     * @param array<mixed> $config
     * @param array<string, string> $queryTags
     * @throws InvalidArgumentException if the value is invalid
     */
    public function __construct(
        array $config = [],
        readonly string $runId = '',
        array $queryTags = [],
        BackOffPolicyInterface|null $backOffPolicy = null,
    ) {
        parent::__construct($config);
        if ($backOffPolicy === null) {
            $this->backOffPolicy = new ExponentialRandomBackOffPolicy(
                ExponentialBackOffPolicy::DEFAULT_INITIAL_INTERVAL,
                1.8,
                10_000, // 10s
            );
        } else {
            $this->backOffPolicy = $backOffPolicy;
        }

        $this->queryTags = new QueryTags($queryTags);
    }

    /**
     * @param array<mixed> $options
     */
    public function runQuery(JobConfigurationInterface $query, array $options = []): QueryResults
    {
        $labels = [];
        if ($this->runId !== '') {
            $labels['run_id'] = $this->runId;
        }

        if ($this->queryTags->isEmpty() === false) {
            $labels = array_merge($labels, $this->queryTags->toArray());
        }

        if (count($labels) !== 0 && method_exists($query, 'labels')) {
            $query = $query->labels($labels);
        }

        return $this->runJob($query, $options)
            ->queryResults();
    }

    /**
     * @param array<mixed> $options
     */
    public function runJob(JobConfigurationInterface $config, array $options = []): Job
    {
        $options += [
            'retryCount' => 5,
            'backOffPolicy' => new ExponentialRandomBackOffPolicy(10, 1.8, 300),
        ];
        assert(is_int($options['retryCount']) && $options['retryCount'] > 0);
        assert($options['backOffPolicy'] instanceof BackOffPolicyInterface);
        $retryPolicy = new SimpleRetryPolicy($options['retryCount']);
        $proxy = new RetryProxy($retryPolicy, $options['backOffPolicy']);
        $job = $proxy->call(function () use ($config, $options): Job {
            return $this->startJob($config, $options);
        });
        assert($job instanceof Job);
        $context = $this->backOffPolicy->start();
        do {
            $this->backOffPolicy->backOff($context);
            $job->reload();
        } while (!$job->isComplete());

        return $job;
    }
}
