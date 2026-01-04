<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column;

use Keboola\Datatype\Definition\Redshift;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Collection;
use Keboola\TableBackendUtils\ColumnException;

/**
 * @extends Collection<ColumnInterface>
 */
final class ColumnCollection extends Collection
{
    /** @var array|int[] */
    protected static array $limits = [
        Redshift::class => 1600,
        // https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_usage.html
        Snowflake::class => 10000,
        // it's complicated with columns limit in SNFLK
        // but standard tables should have limit of 10k columns
        // https://community.snowflake.com/s/question/0D5Do00000Lm9RFKAZ/what-is-the-maximum-number-of-columns-in-a-normal-table-in-snowflake-ive-read-somewhere-that-it-was-a-soft-limit-of-2000-but-that-was-a-few-years-ago-other-sources-have-said-its-now-10000-as-of-2021-does-anyone-have-a-definitive-answer-please
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
                        // we have to force user to create MAX-1 columns, becase we need 1 for timestamp
                        sprintf('Too many columns. Maximum is %s columns.', $limit - 1),
                        ColumnException::STRING_CODE_TO_MANY_COLUMNS,
                    );
                }
            }
        }
    }
}
