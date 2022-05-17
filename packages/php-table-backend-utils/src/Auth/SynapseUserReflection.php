<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;

class SynapseUserReflection implements UserReflectionInterface
{
    private \Doctrine\DBAL\Connection $connection;

    private string $userName;

    public function __construct(Connection $connection, string $userName)
    {
        $this->connection = $connection;
        $this->userName = $userName;
    }

    public function endAllSessions(): void
    {
        $ids = $this->getAllSessionIds();

        foreach ($ids as $id) {
            $this->connection->exec(sprintf(
                'KILL %s;',
                SynapseQuote::quote($id)
            ));
        }
    }

    /**
     * @return string[]
     */
    public function getAllSessionIds(): array
    {
        $sql = <<< EOD
SELECT c.session_id AS id
    FROM sys.dm_pdw_exec_connections AS c  
    JOIN sys.dm_pdw_exec_sessions AS s  
        ON c.session_id = s.session_id  
    WHERE s.login_name = N%s
EOD;
        /** @var string[]|false $sessions */
        $sessions = $this->connection->fetchNumeric(
            sprintf($sql, SynapseQuote::quote($this->userName))
        );

        if ($sessions === false) {
            return [];
        }

        return $sessions;
    }
}
