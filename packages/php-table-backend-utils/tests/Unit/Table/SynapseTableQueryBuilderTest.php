<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\QueryBuilderException;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use PHPUnit\Framework\TestCase;

/**
 * @covers SynapseTableQueryBuilder
 */
class SynapseTableQueryBuilderTest extends TestCase
{
    public function testGetCreateTableCommandTooMuchColumns(): void
    {
        $connMock = $this->createMock(Connection::class);
        $connMock->expects($this->once())->method('getDatabasePlatform')->willReturn(
            $this->createMock(SQLServer2012Platform::class)
        );

        $cols = [];
        for ($i = 0; $i < 1026; $i++) {
            $cols[] = SynapseColumn::createGenericColumn('name' . $i);
        }

        $qb = new SynapseTableQueryBuilder($connMock);
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Too many columns. Maximum is 1024 columns.');
        $qb->getCreateTableCommand('schema', 'table', $cols);
    }

    public function testGetCreateTempTableCommandTooMuchColumns(): void
    {
        $connMock = $this->createMock(Connection::class);
        $connMock->expects($this->once())->method('getDatabasePlatform')->willReturn(
            $this->createMock(SQLServer2012Platform::class)
        );

        $cols = [];
        for ($i = 0; $i < 1026; $i++) {
            $cols[] = SynapseColumn::createGenericColumn('name' . $i);
        }

        $qb = new SynapseTableQueryBuilder($connMock);
        $this->expectException(QueryBuilderException::class);
        $this->expectExceptionMessage('Too many columns. Maximum is 1024 columns.');
        $qb->getCreateTempTableCommand('schema', 'table', $cols);
    }
}
