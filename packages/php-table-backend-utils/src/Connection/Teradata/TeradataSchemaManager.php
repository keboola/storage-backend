<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Exception;

/**
 * @extends AbstractSchemaManager<TeradataPlatform>
 */
class TeradataSchemaManager extends AbstractSchemaManager
{
    /**
     * @param string[] $tableColumn
     * @throws Exception
     */
    // because of compatibility with interface
    // phpcs:ignore SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingParameterTypeHint, SlevomatCodingStandard.TypeHints.TypeHintDeclaration.MissingReturnTypeHint
    protected function _getPortableTableColumnDefinition($tableColumn)
    {
        throw new Exception('method is not implemented yet');

        // TODO: Implement _getPortableTableColumnDefinition() method.
    }
}
