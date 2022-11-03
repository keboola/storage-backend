<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\Connection;

use Doctrine\DBAL\DriverManager;
use Keboola\TableBackendUtils\Connection\Synapse\SynapseDriver;
use Tests\Keboola\TableBackendUtils\Functional\Synapse\SynapseBaseCase;

class WlmContextTest extends SynapseBaseCase
{
    private const FETCH_SESSION_SET_WLM_SQL = <<<EOD
SELECT TOP 1 [command]
FROM [sys].[dm_pdw_exec_requests] AS r LEFT JOIN [sys].[dm_pdw_exec_sessions] AS s ON [s].[session_id]=[r].[session_id]
WHERE [s].[session_id] = '%s' AND [r].[command] LIKE '%%wlm%%'
EOD;

    public function testSynapseWlmContext(): void
    {
        $conn = DriverManager::getConnection([
            'driverClass' => SynapseDriver::class,
            'user' => (string) getenv('SYNAPSE_UID'),
            'password' => (string) getenv('SYNAPSE_PWD'),
            'host' => (string) getenv('SYNAPSE_SERVER'),
            'dbname' => (string) getenv('SYNAPSE_DATABASE'),
            'port' => 1433,
            'driverOptions' => [
                'ConnectRetryCount' => 5,
                'wlm_context' => 'test', // <- set wlm_context
                'ConnectRetryInterval' => 10,
            ],
        ]);

        /** @var string $sessionId1 */
        $sessionId1 = $conn->fetchOne('SELECT SESSION_ID();');
        $fetchSessionSetSql = self::FETCH_SESSION_SET_WLM_SQL;

        $sessionSetQuery = $conn->fetchOne(sprintf($fetchSessionSetSql, $sessionId1));
        $this->assertSame(
            'EXEC sys.sp_set_session_context @key = \'wlm_context\', @value = \'test\'',
            $sessionSetQuery
        );

        // reconnect and check if wlm_context is set on next session
        $conn->close();
        $conn->connect();

        /** @var string $sessionId2 */
        $sessionId2 = $conn->fetchOne('SELECT SESSION_ID();');
        $this->assertNotSame($sessionId1, $sessionId2);
        $sessionSetQuery = $conn->fetchOne(sprintf($fetchSessionSetSql, $sessionId2));
        $this->assertSame(
            'EXEC sys.sp_set_session_context @key = \'wlm_context\', @value = \'test\'',
            $sessionSetQuery
        );
    }

    public function testSynapseWlmContextNotSet(): void
    {
        /** @var string $sessionId1 */
        $sessionId1 = $this->connection->fetchOne('SELECT SESSION_ID();');
        $fetchSessionSetSql = self::FETCH_SESSION_SET_WLM_SQL;

        $sessionSetQuery = $this->connection->fetchOne(sprintf($fetchSessionSetSql, $sessionId1));
        $this->assertNotSame(
            'EXEC sys.sp_set_session_context @key = \'wlm_context\', @value = \'test\'',
            $sessionSetQuery
        );
    }
}
