<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use PHPUnit\Framework\MockObject\MockObject;

trait MockConnectionTrait
{
    /**
     * @return Connection|MockObject
     */
    private function mockConnection()
    {
        /** @var Connection|MockObject $mock */
        $mock = $this->createMock(Connection::class);
        $mock->expects($this->any())->method('getDatabasePlatform')->willReturn(
            new SQLServer2012Platform()
        );
        $mock->expects($this->any())->method('quote')->willReturnCallback(static function ($input) {
            return QuoteHelper::quote($input);
        });

        return $mock;
    }
}
