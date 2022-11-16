<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Synapse;

use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use PHPUnit\Framework\TestCase;

class TableIndexDefinitionTest extends TestCase
{
    public function testValidCCI(): void
    {
        $definition = new TableIndexDefinition('CLUSTERED COLUMNSTORE INDEX');
        self::assertSame([], $definition->getIndexedColumnsNames());
        self::assertSame('CLUSTERED COLUMNSTORE INDEX', $definition->getIndexType());
    }

    public function testValidHEAP(): void
    {
        $definition = new TableIndexDefinition('HEAP');
        self::assertSame([], $definition->getIndexedColumnsNames());
        self::assertSame('HEAP', $definition->getIndexType());
    }

    public function testValidCI(): void
    {
        $definition = new TableIndexDefinition('CLUSTERED INDEX', ['id']);
        self::assertSame(['id'], $definition->getIndexedColumnsNames());
        self::assertSame('CLUSTERED INDEX', $definition->getIndexType());
    }
}
