<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\Datatype\Definition\Redshift;
use Keboola\Datatype\Definition\Synapse;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Collection;
use Keboola\TableBackendUtils\ColumnException;

/**
 * @extends Collection<ColumnInterface>
 */
final class ColumnCollection extends Collection
{
    /** @var array|int[] */
    protected static array $limits = [
        Synapse::class => 1024,
        // https://www.stitchdata.com/docs/destinations/microsoft-azure-synapse-analytics/reference
        Teradata::class => 2048,
        // https://docs.teradata.com/r/Teradata-VantageTM-Database-Design/March-2019/Teradata-System-Limits/Database-Limits
        Redshift::class => 1600,
        // https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_usage.html
    ];

    /**
     * @param ColumnInterface[] $columns
     */
    public function __construct(array $columns)
    {
        $this->assertTableColumnsCount($columns);
        parent::__construct($columns);
    }

    /**
     * @param ColumnInterface[] $columns
     */
    private function assertTableColumnsCount(array $columns): void
    {
        $firstColumn = reset($columns);
        if ($firstColumn) {
            $firstDefinitionClass = get_class($firstColumn->getColumnDefinition());

            if (array_key_exists($firstDefinitionClass, self::$limits)) {
                $limit = self::$limits[$firstDefinitionClass];
                if (count($columns) > $limit) {
                    throw new ColumnException(
                        sprintf('Too many columns. Maximum is %s columns.', $limit),
                        ColumnException::STRING_CODE_TO_MANY_COLUMNS
                    );
                }
            }
        }
    }
}
