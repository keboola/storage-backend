<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

enum TableType: string
{
    case TABLE = 'table';
    case VIEW = 'view';

    // Snowflake|bigquery|redshift external table
    case EXTERNAL = 'external-table';
}
