<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\File;

class Snowflake
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function importTableFromFile(
        ImportOptions $importOptions,
        File\Azure $file
    ): void {
        $createStageSql = sprintf(
            'CREATE TEMPORARY STAGE azstage URL = \'azure://%s.blob.core.windows.net/%s/\' 
            CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')',
            $file->getAccountName(),
            $file->getContainer(),
            $file->getSasToken()
        );
        $this->connection->query($createStageSql);

        $copySql = sprintf(
            'COPY INTO %s.%s FROM @azstage/%s',
            $this->connection->quoteIdentifier($importOptions->getSchema()),
            $this->connection->quoteIdentifier($importOptions->getTableName()),
            $file->getFilePath()
        );
        $this->connection->query($copySql);
    }
}
