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
        $info = $job->info();

        $this->assertArrayHasKey('configuration', $info);
        $this->assertIsArray($info['configuration']);
        $this->assertArrayHasKey('labels', $info['configuration']);
        $this->assertIsArray($info['configuration']['labels']);
        $this->assertArrayHasKey('run_id', $info['configuration']['labels']);
        $this->assertEquals('e2e-utils-lib', $info['configuration']['labels']['run_id']);

        $this->assertArrayHasKey('branch_id', $info['configuration']['labels']);
        $this->assertEquals('1234', $info['configuration']['labels']['branch_id']);
    }
}
