<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Connection;

use GuzzleHttp\Client;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientHandler;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Keboola\TableBackendUtils\Connection\Bigquery\Retry;
use Psr\Log\NullLogger;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class QueryTagsTest extends BigqueryBaseCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testRunIdAndBranchIdQueryTagExists(): void
    {
        $bqClient = new BigQueryClientWrapper(
            [
                'keyFile' => $this->getCredentials(),
                'httpHandler' => new BigQueryClientHandler(new Client()),
                'restRetryFunction' => Retry::getRestRetryFunction(new NullLogger(), true),
            ],
            'e2e-utils-lib',
            [
                'branch_id' => '1234',
            ],
        );

        $query = $bqClient->query('SELECT 1');
        $queryResults = $bqClient->runQuery($query);

        // Get job info and verify the run_id and branch_id label
        $job = $queryResults->job();
        /** @var array<string, mixed> $info */
        $info = $job->info();

        $this->assertArrayHasKey('configuration', $info);
        /** @var array<string, mixed> $configuration */
        $configuration = $info['configuration'];
        $this->assertArrayHasKey('labels', $configuration);
        /** @var array<string, string> $labels */
        $labels = $configuration['labels'];
        $this->assertArrayHasKey('run_id', $labels);
        $this->assertEquals('e2e-utils-lib', $labels['run_id']);

        $this->assertArrayHasKey('branch_id', $labels);
        $this->assertEquals('1234', $labels['branch_id']);
    }
}
