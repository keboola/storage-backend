<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Exasol\Exporter as ExasolExporter;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * exporter is covered by functional tests
 */
class ExporterTest extends TestCase
{
    public function test(): void
    {
        /** @var Connection|MockObject $connection */
        $connection = $this->createMock(Connection::class);
        $exporter= new ExasolExporter($connection);
        self::assertInstanceOf(ExporterInterface::class, $exporter);
    }
}
