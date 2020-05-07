<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;

final class GrantOptions implements GrantOptionsInterface
{
    /** @var bool */
    private $allowGrantOption;

    public function __construct(bool $allowGrantOption = false)
    {
        $this->allowGrantOption = $allowGrantOption;
    }

    public function isAllowGrantOption(): bool
    {
        return $this->allowGrantOption;
    }
}
