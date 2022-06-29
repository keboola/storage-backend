<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\MockObject\MockObject;

trait MockDbalConnectionTrait
{
    /**
     * @return Connection|MockObject
     */
    private function mockConnection()
    {
        return $this->createMock(Connection::class);
    }
}
