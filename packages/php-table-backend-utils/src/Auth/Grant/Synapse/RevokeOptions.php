<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

use Keboola\TableBackendUtils\Auth\Grant\RevokeOptionsInterface;

final class RevokeOptions implements RevokeOptionsInterface
{
    public const OPTION_REVOKE_GRANT_OPTION = true;
    public const OPTION_DONT_REVOKE_GRANT_OPTION = false;
    public const OPTION_REVOKE_CASCADE = true;
    public const OPTION_DONT_REVOKE_CASCADE = false;

    /** @var bool */
    private $allowGrantOption;

    /** @var bool */
    private $isCascade;

    /**
     * parameter $allowGrantOption is kept here and on first place
     * because missing this doesn't make much sense, and could be expected
     * that removing grant option will appear in future version of synapse
     *
     * @param self::OPTION_REVOKE_GRANT_OPTION|self::OPTION_DONT_REVOKE_GRANT_OPTION $allowGrantOption
     * @param self::OPTION_REVOKE_CASCADE|self::OPTION_DONT_REVOKE_CASCADE $isCascade
     */
    public function __construct(
        bool $allowGrantOption = self::OPTION_DONT_REVOKE_GRANT_OPTION,
        bool $isCascade = self::OPTION_DONT_REVOKE_CASCADE
    ) {
        if ($allowGrantOption === true) {
            throw new \Exception('Revoking grant option is not supported on Synapse.');
        }
        $this->allowGrantOption = $allowGrantOption;
        $this->isCascade = $isCascade;
    }

    public function isAllowGrantOption(): bool
    {
        return $this->allowGrantOption;
    }

    public function isCascade(): bool
    {
        return $this->isCascade;
    }
}
