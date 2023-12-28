<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception;

use Keboola\Db\ImportExport\Exception\Exception;

class NoMoreRoomInTDException extends Exception
{

    public function __construct(string $dbName)
    {
        parent::__construct(
            sprintf('No more room in Teradata database %s', $dbName),
        );
    }
}
