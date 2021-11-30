<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column\Snowflake;

use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use PHPUnit\Framework\TestCase;

/**
 * @covers SnowflakeColumn
 */
class SnowflakeColumnTest extends TestCase
{
    public function testCreateGenericColumn(): void
    {
        $col = SnowflakeColumn::createGenericColumn('myCol');
        self::assertEquals('myCol', $col->getColumnName());
        self::assertEquals('VARCHAR NOT NULL', $col->getColumnDefinition()->getSQLDefinition());
        self::assertEquals('VARCHAR', $col->getColumnDefinition()->getType());
        self::assertEquals('\'\'', $col->getColumnDefinition()->getDefault());
    }
}
