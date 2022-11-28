<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

class TableNotExistsReflectionException extends ReflectionException
{
    /**
     * @param string[] $path
     */
    public static function createForTable(array $path): self
    {
        return new self(sprintf(
            'Table %s does not exists.',
            implode(
                ',',
                array_map(
                    fn(string $item) => sprintf('"%s"', $item),
                    $path
                )
            )
        ));
    }
}
