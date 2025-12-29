<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
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

        $mock->expects(self::any())->method('quoteIdentifier')->willReturnCallback(static function ($input) {
            assert(is_string($input));
            return QuoteHelper::quoteIdentifier($input);
        });

        return $mock;
    }
}
