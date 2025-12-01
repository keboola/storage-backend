<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

use Throwable;

class TableNotExistsReflectionException extends ReflectionException
{
    /**
     * @param string[] $path
     */
    public static function createForTable(array $path, ?Throwable $previous = null): self
    {
        return new self(
            message: sprintf(
                'Table %s does not exists.',
                implode(
                    ',',
                    array_map(
                        fn(string $item) => sprintf('"%s"', $item),
                        $path,
                    ),
                ),
            ),
            previous: $previous,
        );
    }
}
