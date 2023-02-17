<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\ImportState;
use PHPUnit\Framework\TestCase;

class ImportStateTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $state = new ImportState('stagingTable');
        self::assertSame('stagingTable', $state->getStagingTableName());
    }

    public function testGetResult(): void
    {
        $state = new ImportState('stagingTable');
        $state->addImportedRowsCount(10);
        $state->startTimer('timer1');
        $state->startTimer('timer2');
        $state->stopTimer('timer1');
        $state->setImportedColumns(['col1', 'col2']);

        $result = $state->getResult();
        self::assertInstanceOf(Result::class, $result);
        self::assertEquals(10, $result->getImportedRowsCount());
        self::assertSame(['col1', 'col2'], $result->getImportedColumns());
        self::assertCount(2, $result->getTimers());
        self::assertEquals('timer1', $result->getTimers()[0]['name']);
        self::assertSame([
            'name' => 'timer2',
            'durationSeconds' => null,
        ], $result->getTimers()[1]);
        self::assertSame([], $result->getWarnings());
    }
}
