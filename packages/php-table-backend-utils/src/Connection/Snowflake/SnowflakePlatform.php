<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Exception;

class SnowflakePlatform extends AbstractPlatform
{
    public function getBooleanTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBooleanTypeDeclarationSQL() method.
    }

    public function getIntegerTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getIntegerTypeDeclarationSQL() method.
    }

    public function getBigIntTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBigIntTypeDeclarationSQL() method.
    }

    public function getSmallIntTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getSmallIntTypeDeclarationSQL() method.
    }

    protected function _getCommonIntegerTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement _getCommonIntegerTypeDeclarationSQL() method.
    }

    protected function initializeDoctrineTypeMappings()
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement initializeDoctrineTypeMappings() method.
    }

    public function getClobTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getClobTypeDeclarationSQL() method.
    }

    public function getBlobTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBlobTypeDeclarationSQL() method.
    }

    public function getName()
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getName() method.
    }

    public function getCurrentDatabaseExpression(): string
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getCurrentDatabaseExpression() method.
    }
}
