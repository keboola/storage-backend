<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Connection;

use Generator;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\TableBackendUtils\Functional\Bigquery\BigqueryBaseCase;

class SessionTest extends BigqueryBaseCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    public function testCreateSession(): void
    {
        $s = new SessionFactory($this->bqClient);
        $session = $s->createSession();

        $sessionIdDecoded = base64_decode($session->getSessionId());
        // session id is base64 string which contains <project>$<uuid>
        $this->assertIsString($sessionIdDecoded);

        // plain session use
        $job = $this->bqClient->runJob($this->bqClient->query('SELECT 1', [
            'configuration' => [
                'query' => [
                    'connectionProperties' => [
                        'key' => 'session_id',
                        'value' => $session->getSessionId(),
                    ],
                ],
            ],
        ]));

        $testSessionPlain = Session::createFromJob($job);
        $this->assertSame($session->getSessionId(), $testSessionPlain->getSessionId());

        // use session with helper method
        $job = $this->bqClient->runJob($this->bqClient->query('SELECT 1', $session->getAsQueryOptions()));
        $testSessionHelper = Session::createFromJob($job);
        $this->assertSame($session->getSessionId(), $testSessionHelper->getSessionId());
    }
}
