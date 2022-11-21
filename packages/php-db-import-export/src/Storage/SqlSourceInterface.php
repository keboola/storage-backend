<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

interface SqlSourceInterface extends SourceInterface
{
    public function getFromStatement(): string;

    public function getFromStatementWithStringCasting(): string;

    /** @return array<mixed> */
    public function getQueryBindings(): array;
}
