<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Bigquery\Connection;

use Keboola\TableBackendUtils\Connection\Bigquery\Session;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
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

        // session id is base64 string which contains <project>$<uuid>
        $sessionIdDecoded = base64_decode($session->getSessionId());
        $this->assertStringContainsString('$', $sessionIdDecoded);

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
