<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

interface SqlSourceInterface extends SourceInterface
{
    public function getFromStatement(): string;
}
