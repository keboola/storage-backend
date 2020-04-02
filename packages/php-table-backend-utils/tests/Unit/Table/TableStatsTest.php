<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table;

use Keboola\TableBackendUtils\Table\TableStats;
use PHPUnit\Framework\TestCase;

class TableStatsTest extends TestCase
{
    public function testGetters(): void
    {
        $stats = new TableStats(10, 10);
        $this->assertEquals(10, $stats->getRowsCount());
        $this->assertEquals(10, $stats->getDataSizeBytes());
    }
}
