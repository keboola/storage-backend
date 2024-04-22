<?php

declare(strict_types=1);

namespace Functional\Bigquery\Connection;

use Keboola\TableBackendUtils\Connection\Bigquery\QueryExecutor;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

/**
 * @covers QueryExecutor
 */
class QueryExecutorTest extends BigqueryBaseCase
{
    public function testRunQuery(): void
    {
        $queryExecutor = new QueryExecutor($this->bqClient, '123');
        $result = $queryExecutor->runQuery($this->bqClient->query('SELECT 1'));
        $this->assertSame('123', $result->job()->info()['configuration']['labels']['run_id']);
        $this->assertSame([['f0_' => 1]], iterator_to_array($result->getIterator()));
    }

    public function testRunJob(): void
    {
        $queryExecutor = new QueryExecutor($this->bqClient, '123');
        $result = $queryExecutor->runJob($this->bqClient->query('SELECT 1'));
        $this->assertSame('123', $result->info()['configuration']['labels']['run_id']);
    }
}
