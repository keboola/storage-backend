<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend;

use Exception;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Db\ImportExport\Backend\SourceDestinationColumnMap;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use PHPUnit\Framework\TestCase;
use Throwable;

/**
 * Class SourceDestinationColumnMapTest.
 *
 * @covers \Keboola\Db\ImportExport\Backend\SourceDestinationColumnMap
 */
final class SourceDestinationColumnMapTest extends TestCase
{
    /**
     * @return array{ColumnInterface, ColumnInterface, SourceDestinationColumnMap}
     */
    private function getMap(): array
    {
        $col1 = $this->getColumn('col1', 'string');
        $source = new ColumnCollection([
            $col1,
            $this->getColumn('col2', 'string'),
        ]);
        $col1Dest = $this->getColumn('col1', 'bool');
        $destination = new ColumnCollection([
            $col1Dest,
            $this->getColumn('col2', 'bool'),
        ]);

        $map = new SourceDestinationColumnMap(
            $source,
            $destination
        );
        return [$col1, $col1Dest, $map];
    }

    private function getColumn(string $name, string $type): ColumnInterface
    {
        return new class($name, $type) implements ColumnInterface {
            public function __construct(private readonly string $name, private readonly string $type)
            {
            }

            public function getColumnName(): string
            {
                return $this->name;
            }

            public function getColumnDefinition(): DefinitionInterface
            {
                return new class($this->type) implements DefinitionInterface {
                    public function __construct(private readonly string $type)
                    {
                    }

                    public function getSQLDefinition(): string
                    {
                        return $this->type . 'DEF';
                    }

                    public function toArray(): array
                    {
                        throw new Exception('Not implemented');
                    }

                    public function getBasetype(): string
                    {
                        throw new Exception('Not implemented');
                    }

                    public function getType(): string
                    {
                        return $this->type;
                    }

                    public function getLength(): ?string
                    {
                        throw new Exception('Not implemented');
                    }

                    public function isNullable(): bool
                    {
                        throw new Exception('Not implemented');
                    }

                    public function getDefault(): ?string
                    {
                        throw new Exception('Not implemented');
                    }

                    public static function getTypeByBasetype(string $basetype): string
                    {
                        throw new Exception('Not implemented');
                    }
                };
            }

            public static function createGenericColumn(string $columnName): ColumnInterface
            {
                throw new Exception('Not implemented');
            }

            public static function createTimestampColumn(
                string $columnName = self::TIMESTAMP_COLUMN_NAME
            ): ColumnInterface {
                throw new Exception('Not implemented');
            }

            /**
             * @param array<mixed> $dbResponse
             */
            public static function createFromDB(array $dbResponse): ColumnInterface
            {
                throw new Exception('Not implemented');
            }
        };
    }

    public function testCreateForTables(): void
    {
        $col1 = $this->getColumn('col1', 'string');
        $source = $this->createMock(TableDefinitionInterface::class);
        $source->expects(self::once())->method('getColumnsDefinitions')->willReturn(new ColumnCollection([
            $col1,
            $this->getColumn('col2', 'string'),
        ]));
        $col1Dest = $this->getColumn('col1', 'bool');
        $destination = $this->createMock(TableDefinitionInterface::class);
        $destination->expects(self::once())->method('getColumnsDefinitions')->willReturn(new ColumnCollection([
            $col1Dest,
            $this->getColumn('col2', 'bool'),
        ]));

        $map = SourceDestinationColumnMap::createForTables(
            $source,
            $destination
        );

        $this->assertSame($col1Dest, $map->getDestination($col1));
    }

    public function testCreateForCollection(): void
    {
        [$col1, $col1Dest, $map] = $this->getMap();

        $this->assertSame($col1Dest, $map->getDestination($col1));
    }

    public function testColumnMismatch(): void
    {
        $source = new ColumnCollection([
            $this->getColumn('col1', 'string'),
            $this->getColumn('col2', 'string'),
        ]);
        $destination = new ColumnCollection([
            $this->getColumn('col1', 'bool'),
            $this->getColumn('col2', 'bool'),
            $this->getColumn('col3', 'bool'),
        ]);

        $this->expectException(ColumnsMismatchException::class);
        new SourceDestinationColumnMap(
            $source,
            $destination
        );
    }

    public function testIgnoreColumn(): void
    {
        $this->expectNotToPerformAssertions();
        $source = new ColumnCollection([
            $this->getColumn('col1', 'string'),
            $this->getColumn('col2', 'string'),
        ]);
        $destination = new ColumnCollection([
            $this->getColumn('col1', 'bool'),
            $this->getColumn('col2', 'bool'),
            $this->getColumn('col3', 'bool'),
        ]);

        new SourceDestinationColumnMap(
            $source,
            $destination,
            ['col3']
        );
    }

    public function testColumnNotFound(): void
    {
        [, , $map] = $this->getMap();

        $this->expectException(Throwable::class);
        $map->getDestination($this->getColumn('test', 'string'));
    }
}
