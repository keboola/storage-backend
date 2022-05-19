<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\Exception;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

final class Assert
{
    // TODO add some inheritance on synapse/exasol assert
    public static function assertColumnsOnTableDefinition(
        SourceInterface $source,
        ExasolTableDefinition $destinationDefinition
    ): void {
        if (count($source->getColumnsNames()) === 0) {
            throw new Exception(
                'No columns found in CSV file.',
                Exception::NO_COLUMNS
            );
        }

        $moreColumns = array_diff($source->getColumnsNames(), $destinationDefinition->getColumnsNames());
        if (!empty($moreColumns)) {
            throw new Exception(
                'Columns doest not match. Non existing columns: ' . implode(', ', $moreColumns),
                Exception::COLUMNS_COUNT_NOT_MATCH
            );
        }
    }
}
