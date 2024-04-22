<?php

declare(strict_types=1);

namespace Unit\Connection\BigQuery;

use Generator;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Job;
use Google\Cloud\BigQuery\JobConfigurationInterface;
use Google\Cloud\BigQuery\QueryJobConfiguration;
use Google\Cloud\BigQuery\QueryResults;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryExecutor;
use PHPUnit\Framework\TestCase;
use Retry\BackOff\BackOffContextInterface;
use Retry\BackOff\StatelessBackOffPolicy;

/**
 * @covers QueryExecutor
 */
class QueryExecutorTest extends TestCase
{
    public function runIdProvider(): Generator
    {
        yield 'empty runId' => [''];
        yield 'non-empty runId' => ['runId'];
    }

    /**
     * @dataProvider runIdProvider
     */
    public function testExecuteQueryBackOff(string $runId): void
    {
        $client = $this->createMock(BigQueryClient::class);
        $query = $this->createMock(QueryJobConfiguration::class);
        $job = $this->createMock(Job::class);
        $expectedResult = $this->createMock(QueryResults::class);
        $job->expects(self::once())->method('queryResults')->willReturn($expectedResult);
        $jobCallCount = 0;
        $job->expects(self::exactly(3))->method('isComplete')->willReturnCallback(
            function () use (&$jobCallCount) {
                $jobCallCount++;
                return $jobCallCount === 3;
            },
        );
        $job->expects(self::exactly(3))->method('reload');

        if ($runId !== '') {
            $query->expects(self::once())->method('labels')->with(['run_id' => $runId])->willReturn($query);
        } else {
            $query->expects(self::never())->method('labels');
        }
        $client->expects(self::once())->method('startQuery')->with($query)->willReturn($job);
        $backOff = $this->createMock(StatelessBackOffPolicy::class);
        $ctx = $this->createMock(BackOffContextInterface::class);
        $backOff->expects(self::once())->method('start')->willReturn($ctx);
        $backOff->expects(self::exactly(3))->method('backOff')->with($ctx);

        $queryExecutor = new QueryExecutor($client, $runId, $backOff);
        $result = $queryExecutor->runQuery($query);
        $this->assertSame($expectedResult, $result);
    }

    /**
     * @dataProvider runIdProvider
     */
    public function testRunJob(string $runId): void
    {
        $client = $this->createMock(BigQueryClient::class);
        $query = $this->createMock(QueryJobConfiguration::class);
        $expectedJob = $this->createMock(Job::class);

        $client->expects(self::once())->method('runJob')->with($query)->willReturn($expectedJob);

        if ($runId !== '') {
            $query->expects(self::once())->method('labels')->with(['run_id' => $runId])->willReturn($query);
        } else {
            $query->expects(self::never())->method('labels');
        }

        $queryExecutor = new QueryExecutor($client, $runId);
        $result = $queryExecutor->runJob($query);
        $this->assertSame($expectedJob, $result);
    }
}
