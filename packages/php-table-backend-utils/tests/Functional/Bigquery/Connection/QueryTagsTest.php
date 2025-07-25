<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Connection;

use GuzzleHttp\Client;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientHandler;
use Keboola\TableBackendUtils\Connection\Bigquery\BigQueryClientWrapper;
use Keboola\TableBackendUtils\Connection\Bigquery\Retry;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;
use Psr\Log\NullLogger;

class QueryTagsTest extends BigqueryBaseCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testRunIdAndBranchIdQueryTagIsPresent(): void
    {
        $bqClient = new BigQueryClientWrapper(
            [
                'keyFile' => $this->getCredentials(),
                'httpHandler' => new BigQueryClientHandler(new Client()),
                'restRetryFunction' => Retry::getRestRetryFunction(new NullLogger(), true),
            ],
            'e2e-utils-lib',
        );

        $query = $this->bqClient->query('SELECT 1');
        $queryResults = $bqClient->runQuery($query);

        // Get job info and verify the run_id and branch_id label
        $job = $queryResults->job();
        $info = $job->info();

        $this->assertArrayHasKey('configuration', $info);
        $this->assertArrayHasKey('labels', $info['configuration']);
        $this->assertArrayHasKey('run_id', $info['configuration']['labels']);
        $this->assertEquals('e2e-utils-lib', $info['configuration']['labels']['run_id']);

    }
}