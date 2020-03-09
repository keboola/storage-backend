<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Synapse;

use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Synapse\SynapseImportAdapter;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SynapseAdapterTest extends BaseTestCase
{
    public function testGetCopyCommands(): void
    {
        /** @var Storage\Synapse\Table|MockObject $source */
        $source = self::createMock(Storage\Synapse\Table::class);
        $source->expects(self::once())->method('getSchema')->willReturn('schema');
        $source->expects(self::once())->method('getTableName')->willReturn('table');

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions([], ['col1', 'col2']);
        $adapter = new SynapseImportAdapter($source, new SQLServer2012Platform());
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable'
        );

        self::assertSame([
            'INSERT INTO [schema].[stagingTable] ([col1], [col2]) SELECT [col1], [col2] FROM [schema].[table]',
        ], $commands);
    }
}
