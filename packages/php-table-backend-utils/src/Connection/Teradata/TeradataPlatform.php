<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

class TeradataPlatform extends \Doctrine\DBAL\Platforms\AbstractPlatform
{
    /**
     * @inheritDoc
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef)
    {
        throw new \Exception('method is not implemented yet');
        // TODO: Implement getBooleanTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getIntegerTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getBigIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getSmallIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement _getCommonIntegerTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    protected function initializeDoctrineTypeMappings()
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement initializeDoctrineTypeMappings() method.
    }

    /**
     * @inheritDoc
     */
    public function getClobTypeDeclarationSQL(array $field)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getClobTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBlobTypeDeclarationSQL(array $field)
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getBlobTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getName()
    {
        throw new \Exception('method is not implemented yet');

        // TODO: Implement getName() method.
    }
}
