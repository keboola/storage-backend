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

    public function getType(): string;

    public function getLength(): ?string;

    public function isNullable(): bool;

    public function getDefault(): ?string;

    public static function getTypeByBasetype(string $basetype): string;

    public function isSameType(DefinitionInterface $definition): bool;
}
