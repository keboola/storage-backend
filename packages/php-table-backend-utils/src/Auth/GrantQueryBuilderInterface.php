<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;
use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;

interface GrantQueryBuilderInterface
{
    public function getGrantSql(GrantOptionsInterface $options): string;

    public function getRevokeSql(RevokeOptionsInterface $options): string;
}
