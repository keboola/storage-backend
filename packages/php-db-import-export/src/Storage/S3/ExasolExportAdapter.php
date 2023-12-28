<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\S3;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;

class ExasolExportAdapter implements BackendExportAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public static function isSupported(Storage\SourceInterface $source, Storage\DestinationInterface $destination): bool
    {
        if (!$source instanceof Storage\SqlSourceInterface) {
            return false;
        }
        if (!$destination instanceof Storage\S3\DestinationFile) {
            return false;
        }
        return true;
    }

    /**
     * @return array<mixed>
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $exportOptions,
    ): array {
        assert($source instanceof Storage\SqlSourceInterface);
        assert($destination instanceof DestinationFile);
        $files = $destination->getSlicedFilesNames($exportOptions->isCompressed());

        $suffix = $exportOptions->isCompressed() ? '.gz' : '';
        $entries = array_map(
            static function ($entry) use ($suffix) {
                return 'FILE ' . ExasolQuote::quote($entry . $suffix);
            },
            $files,
        );

        $sql = sprintf(
            <<<EOD
EXPORT %s INTO CSV AT '%s' 
USER '%s' IDENTIFIED BY '%s;sse_type=AES256'
%s
DELIMIT = ALWAYS
REPLACE
EOD
            ,
            $source->getFromStatement(),
            $destination->getBucketURL(),
            $destination->getKey(),
            $destination->getSecret(),
            implode("\n", $entries),
        );
        $this->connection->executeStatement($sql, $source->getQueryBindings());

        if ($exportOptions->generateManifest()) {
            (new Storage\S3\ManifestGenerator\S3SlicedManifestFromFolderGenerator($destination->getClient()))
                ->generateAndSaveManifest($destination->getRelativePath());
        }

        return [];
    }
}
