<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\GrantOptionsInterface;

final class GrantOptions implements GrantOptionsInterface
{
    public const OPTION_ALLOW_GRANT_OPTION = true;
    public const OPTION_DONT_ALLOW_GRANT_OPTION = false;

    /** @var bool */
    private $allowGrantOption;

    /**
     * @param self::OPTION_ALLOW_GRANT_OPTION|self::OPTION_DONT_ALLOW_GRANT_OPTION $allowGrantOption
     */
    public function __construct(bool $allowGrantOption = self::OPTION_DONT_ALLOW_GRANT_OPTION)
    {
        $this->allowGrantOption = $allowGrantOption;
    }

    public function isAllowGrantOption(): bool
    {
        return $this->allowGrantOption;
    }
}
