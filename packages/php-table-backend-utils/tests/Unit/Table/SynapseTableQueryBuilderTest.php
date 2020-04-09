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
        // phpcs:ignore
        $this->expectExceptionMessage('Temporary table name invalid, temporary table name must start with "#" a not be empty "table" supplied.');
        $qb->getCreateTempTableCommand('schema', 'table', new ColumnCollection([]));
    }

    public function testGetCreateTempTableCommandNotValidName(): void
    {
        $connMock = $this->createMock(Connection::class);
        $connMock->expects($this->once())->method('getDatabasePlatform')->willReturn(
            $this->createMock(SQLServer2012Platform::class)
        );

        $qb = new SynapseTableQueryBuilder($connMock);
        $this->expectException(QueryBuilderException::class);
        // phpcs:ignore
        $this->expectExceptionMessage('Temporary table name invalid, temporary table name must start with "#" a not be empty "#" supplied.');
        $qb->getCreateTempTableCommand('schema', '#', new ColumnCollection([]));
    }
}
