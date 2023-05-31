<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

enum TableType: string
{
    case TABLE = 'table';
    case VIEW = 'view';
    // Snowflake external table
    case SNOWFLAKE_EXTERNAL = 'snowflake-external-table';
}
