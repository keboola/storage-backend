<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\ColumnException;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testGetIterator(): void
    {
        $collection = new ColumnCollection([
            SnowflakeColumn::createGenericColumn('col1'),
            SnowflakeColumn::createGenericColumn('col2'),
        ]);

        $this->assertIsIterable($collection);
    }

    /**
     * @dataProvider  tooMuchColumnsProviderWithLimits
     * @param class-string<SnowflakeColumn> $definitionClass
     */
    public function testTooMuchColumns(string $definitionClass, int $limit): void
    {
        $cols = [];
        for ($i = 0; $i < $limit + 2; $i++) {
            $cols[] = $definitionClass::createGenericColumn('name' . $i);
        }

        $this->expectException(ColumnException::class);
        $this->expectExceptionMessage(sprintf('Too many columns. Maximum is %s columns.', $limit - 1));
        new ColumnCollection($cols);
    }

    public function testCount(): void
    {
        $collection = new ColumnCollection([
            SnowflakeColumn::createGenericColumn('col1'),
            SnowflakeColumn::createGenericColumn('col2'),
        ]);
        $this->assertCount(2, $collection);
    }

    /**
     * @return array<string, array{string, int}>
     */
    public function tooMuchColumnsProviderWithLimits(): array
    {
        return
            [
                'snowflake' => [
                    SnowflakeColumn::class,
                    10000,
                ],
            ];
    }
}
