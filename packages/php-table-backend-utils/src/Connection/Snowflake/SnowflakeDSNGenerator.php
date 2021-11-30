<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

class SnowflakeDSNGenerator
{
    /**
     * The connection constructor accepts the following options:
     * - host (string, required) - hostname
     * - port (int, optional) - port - default 443
     * - warehouse (string) - default warehouse to use
     * - database (string) - default database to use
     * - tracing (int) - the level of detail to be logged in the driver trace files
     * - loginTimeout (int) - Specifies how long to wait for a response when
     * connecting to the Snowflake service before returning a login failure
     * error.
     * - networkTimeout (int) - Specifies how long to wait for a response when
     * interacting with the Snowflake service before returning an error. Zero
     * (0) indicates no network timeout is set.
     * - queryTimeout (int) - Specifies how long to wait for a query to complete
     * before returning an error. Zero (0) indicates to wait indefinitely.
     * - clientSessionKeepAlive (bool) - Parameter that indicates whether to force a user
     * to log in again after a period of inactivity in the session
     * @param array{
     *     'host':string,
     *     'port'?:string,
     *     'warehouse'?:string,
     *     'database'?:string,
     *     'schema'?:string,
     *     'tracing'?:int,
     *     'loginTimeout'?:int,
     *     'networkTimeout'?:int,
     *     'queryTimeout'?: int,
     *     'clientSessionKeepAlive'?: bool,
     *     'maxBackoffAttempts'?:int
     * } $options
     */
    public static function generateDSN(array $options): string
    {
        $requiredOptions = [
            'host',
            'user',
            'password',
        ];

        $allowedOptions = [
            'host',
            'user',
            'password',
            'port',
            'tracing',
            'loginTimeout',
            'networkTimeout',
            'queryTimeout',
            'maxBackoffAttempts',
            'database',
            'schema',
            'warehouse',
            'runId',
            'clientSessionKeepAlive',
            'driverClass',
            'driverOptions',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new \LogicException('Missing options: ' . implode(', ', $missingOptions));
        }

        $unknownOptions = array_diff(array_keys($options), $allowedOptions);
        if (!empty($unknownOptions)) {
            throw new \LogicException('Unknown options: ' . implode(', ', $unknownOptions));
        }

        $port = (isset($options['port']) && $options['port']) ? (int) $options['port'] : 443;
        $tracing = isset($options['tracing']) ? (int) $options['tracing'] : 0;

        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $options['host'];
        $dsn .= ';Port=' . $port;
        $dsn .= ';Tracing=' . $tracing;

        if (isset($options['loginTimeout'])) {
            $dsn .= ';Login_timeout=' . (int) $options['loginTimeout'];
        }

        if (isset($options['networkTimeout'])) {
            $dsn .= ';Network_timeout=' . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ';Query_timeout=' . (int) $options['queryTimeout'];
        }

        if (isset($options['database'])) {
            $dsn .= ';Database=' . SnowflakeQuote::quoteSingleIdentifier($options['database']);
        }

        if (isset($options['schema'])) {
            $dsn .= ';Schema=' . SnowflakeQuote::quoteSingleIdentifier($options['schema']);
        }

        if (isset($options['warehouse'])) {
            $dsn .= ';Warehouse=' . SnowflakeQuote::quoteSingleIdentifier($options['warehouse']);
        }

        if (isset($options['clientSessionKeepAlive']) && $options['clientSessionKeepAlive']) {
            $dsn .= ';CLIENT_SESSION_KEEP_ALIVE=TRUE';
        }

        return $dsn;
    }
}
