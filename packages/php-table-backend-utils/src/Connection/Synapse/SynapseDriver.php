<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Synapse;

use Doctrine\DBAL\Driver\AbstractSQLServerDriver;
use Doctrine\DBAL\Driver\AbstractSQLServerDriver\Exception\PortWithoutHost;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Driver\PDO\Connection as PDOConnection;
use Doctrine\DBAL\Driver\PDO\Exception as PDOException;
use Doctrine\DBAL\Driver\PDO\SQLSrv\Connection;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use PDO;

use PDOException as NativePdoException;
use function is_int;
use function sprintf;

/**
 * Based on Doctrine\DBAL\Driver\PDO\SQLSrv\Driver
 */
class SynapseDriver extends AbstractSQLServerDriver
{
    /**
     * {@inheritdoc}
     *
     * @return \Doctrine\DBAL\Driver\PDO\SQLSrv\Connection
     */
    public function connect(array $params)
    {
        $driverOptions = $dsnOptions = [];
        $wlmContext = null;

        if (isset($params['driverOptions']) && is_array($params['driverOptions'])) {
            /**
             * @var string|int $option
             * @var string $value
             */
            foreach ($params['driverOptions'] as $option => $value) {
                if ($option === 'wlm_context') {
                    $wlmContext = $value;
                    continue;
                }
                if (is_int($option)) {
                    $driverOptions[$option] = $value;
                } else {
                    $dsnOptions[$option] = $value;
                }
            }
        }

        if (!empty($params['persistent'])) {
            $driverOptions[PDO::ATTR_PERSISTENT] = true;
        }

        try {
            $pdo = new PDO(
                $this->constructDsn($params, $dsnOptions),
                $params['user'] ?? '',
                $params['password'] ?? '',
                $driverOptions,
            );
        } catch (NativePdoException $exception) {
            throw PDOException::new($exception);
        }

        $conn = new Connection(new PDOConnection($pdo));
        if ($wlmContext !== null) {
            // set automatically wlm_context on each reconnect
            $conn->exec(
                sprintf(
                    'EXEC sys.sp_set_session_context @key = \'wlm_context\', @value = %s',
                    SynapseQuote::quote($wlmContext),
                ),
            );
        }
        return $conn;
    }

    /**
     * Constructs the Sqlsrv PDO DSN.
     *
     * @param mixed[] $params
     * @param string[] $connectionOptions
     *
     * @throws Exception
     */
    private function constructDsn(array $params, array $connectionOptions): string
    {
        $dsn = 'sqlsrv:server=';

        if (isset($params['host'])) {
            $dsn .= $params['host'];

            if (isset($params['port'])) {
                $dsn .= ',' . $params['port'];
            }
        } elseif (isset($params['port'])) {
            throw PortWithoutHost::new();
        }

        if (isset($params['dbname'])) {
            $connectionOptions['Database'] = $params['dbname'];
        }

        if (isset($params['MultipleActiveResultSets'])) {
            $connectionOptions['MultipleActiveResultSets'] = $params['MultipleActiveResultSets'] ? 'true' : 'false';
        }

        return $dsn . $this->getConnectionOptionsDsn($connectionOptions);
    }

    /**
     * Converts a connection options array to the DSN
     *
     * @param string[] $connectionOptions
     */
    private function getConnectionOptionsDsn(array $connectionOptions): string
    {
        $connectionOptionsDsn = '';

        foreach ($connectionOptions as $paramName => $paramValue) {
            $connectionOptionsDsn .= sprintf(';%s=%s', $paramName, $paramValue);
        }

        return $connectionOptionsDsn;
    }
}
