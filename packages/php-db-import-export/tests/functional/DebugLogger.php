<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

use Psr\Log\AbstractLogger;

class DebugLogger extends AbstractLogger
{
    /**
     * @inheritDoc
     */
    public function log($level, $message, array $context = []): void
    {
        assert(is_string($level));
        error_log(
            sprintf(
                '%s: %s',
                strtoupper($level),
                $this->interpolate($message, $context)
            )
        );
    }

    /**
     * @param mixed[] $context
     */
    protected function interpolate(string $message, array $context = []): string
    {
        // build a replacement array with braces around the context keys
        $replace = [];
        foreach ($context as $key => $val) {
            if (is_scalar($val)) {
                $replace['{' . $key . '}'] = $val;
            }
        }

        // interpolate replacement values into the message and return
        return strtr($message, $replace);
    }
}
