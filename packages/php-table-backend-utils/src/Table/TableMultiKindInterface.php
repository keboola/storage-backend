<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

interface TableMultiKindInterface
{
    public function getKind(): TableKind;
}
