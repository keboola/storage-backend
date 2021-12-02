<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Snowflake;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Exception;

class SnowflakePlatform extends AbstractPlatform
{
    /**
     * @inheritDoc
     */
    public function getBooleanTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBooleanTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getIntegerTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getIntegerTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBigIntTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBigIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getSmallIntTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getSmallIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement _getCommonIntegerTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    protected function initializeDoctrineTypeMappings()
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement initializeDoctrineTypeMappings() method.
    }

    /**
     * @inheritDoc
     */
    public function getClobTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getClobTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBlobTypeDeclarationSQL(array $column)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBlobTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getName()
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getName() method.
    }

    /**
     * @inheritDoc
     */
    public function getCurrentDatabaseExpression(): string
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getCurrentDatabaseExpression() method.
    }
}
