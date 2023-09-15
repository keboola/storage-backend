<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

interface DefinitionInterface
{
    public function getSQLDefinition(): string;

    /**
     * @return array{type:string, length:string|null, nullable:bool, compression?:mixed}
     */
    public function toArray(): array;

    public function getBasetype(): string;

    /**
     * @return string[]
     */
    public static function getTypesAllowedInFilters(): array;

    public static function getTypeByBasetype(string $basetype): string;
}
