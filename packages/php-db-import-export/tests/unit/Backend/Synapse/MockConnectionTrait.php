<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse;

use Doctrine\DBAL\Connection;
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
        return $mock;
    }
}
