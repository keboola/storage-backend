<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared;

interface BackendSupportsInterface
{
    public const BACKEND_REDSHIFT = 'redshift';
    public const BACKEND_SNOWFLAKE = 'snowflake';
    public const BACKEND_SYNAPSE = 'synapse';
    public const BACKEND_EXASOL = 'exasol';
    public const BACKEND_TERADATA = 'teradata';
    public const BACKEND_BIGQUERY = 'bigquery';

    public const SUPPORTED_BACKENDS = [
        self::BACKEND_REDSHIFT,
        self::BACKEND_SNOWFLAKE,
        self::BACKEND_SYNAPSE,
        self::BACKEND_EXASOL,
        self::BACKEND_TERADATA,
        self::BACKEND_BIGQUERY,
    ];
    public function supportsBackend(string $backendName): bool;
}
