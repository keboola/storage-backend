<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;

final class ColumnsHelper
{
    /**
     * @param string[] $columns
     */
    public static function getColumnsString(
        array $columns,
        string $delimiter = ', ',
        ?string $tableAlias = null
    ): string {
        return implode($delimiter, array_map(function ($columns) use (
            $tableAlias
        ) {
            $alias = $tableAlias === null ? '' : $tableAlias . '.';
            return $alias . QuoteHelper::quoteIdentifier($columns);
        }, $columns));
    }
}
