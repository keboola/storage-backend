<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use PHPUnit\Framework\TestCase;

/**
 * @covers SynapseTableQueryBuilder
 */
class SynapseTableQueryBuilderTest extends TestCase
{

    public function testGetCreateTempTableCommandNotTemporary(): void
    {
        $connMock = $this->createMock(Connection::class);
        $connMock->expects($this->once())->method('getDatabasePlatform')->willReturn(
            $this->createMock(SQLServer2012Platform::class)
        );

        $qb = new SynapseTableQueryBuilder($connMock);
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Staging table must start with "#" table name "table" supplied.');
        $qb->getCreateTempTableCommand('schema', 'table', new ColumnCollection([]));
    }
}
