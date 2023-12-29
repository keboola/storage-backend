<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Teradata;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Exception;
use LogicException;
use RuntimeException;

class TeradataPlatform extends AbstractPlatform
{
    /**
     * @inheritDoc
     */
    public function getBooleanTypeDeclarationSQL(array $columnDef)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBooleanTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getIntegerTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBigIntTypeDeclarationSQL(array $columnDef)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getBigIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getSmallIntTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
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
    public function getClobTypeDeclarationSQL(array $field)
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getClobTypeDeclarationSQL() method.
    }

    /**
     * @inheritDoc
     */
    public function getBlobTypeDeclarationSQL(array $field)
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

    public function getCurrentDatabaseExpression(): string
    {
        throw new Exception('method is not implemented yet');
        // TODO: Implement getCurrentDatabaseExpression() method.
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset): string
    {
        if (($limit === null || $limit === 0) && $offset <= 0) {
            return $query;
        }

        if (preg_match('/^\s*SELECT\s+DISTINCT/im', $query) > 0) {
            /** @codingStandardsIgnoreStart */
            /*
            You cannot specify the TOP n operator in any of these SQL statements or statement components:
              - Correlated subquery
              - Subquery in a search condition
              - CREATE JOIN INDEX
              - CREATE HASH INDEX
              - Seed statement or recursive statement in a CREATE RECURSIVE VIEW statement or WITH RECURSIVE statement modifier
              - Subselects of set operations.
            You cannot specify these options in a SELECT statement that specifies the TOP n operator:
              - DISTINCT option
              - QUALIFY clause
              - SAMPLE clause
              - WITH clause
              - This restriction refers to the WITH clause you can specify for summary lines and breaks. See WITH Clause. The nonrecursive WITH statement modifier that can precede the SELECT keyword can be included in statements that also specify the TOP n operator. See WITH Modifier.
              - ORDER BY clause where the sort expression is an ordered analytical function.
            See https://www.docs.teradata.com/r/Teradata-VantageTM-SQL-Data-Manipulation-Language/July-2021/SELECT-Statements/Select-List-Syntax/TOP-Clause/Usage-Notes/Rules-and-Restrictions-for-the-TOP-n-Operator
            */
            /** @codingStandardsIgnoreEnd */
            // skip limit
        } else {
            $query = preg_replace(
                '/^(\s*SELECT\b)/im',
                sprintf('$1 TOP %s', $limit),
                $query,
            );
            if ($query === null) {
                throw new RuntimeException('Adding LIMIT to SQL retunrs error.');
            }
        }

        if ($offset > 0) {
            throw new LogicException('Support for OFFSET not implemented yet.');
        }

        return $query;
    }
}
