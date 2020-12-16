<?php

declare(strict_types=1);

namespace Keboola\Provisioning;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class DeleteSynapse extends BaseCmd
{
    private const OPTION_AZURE_RESOURCE_GROUP = 'resourceGroup';
    private const OPTION_FILE_STORAGE_RESOURCE_ID = 'fileStorageResourceId';
    private const OPTION_SYNAPSE_DW_SERVER_NAME = 'synapseDwServerName';
    private const OPTION_SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID = 'synapseServicePrincipalObjectId';
    private const OPTION_SYNAPSE_SQL_SERVER_NAME = 'synapseSqlServerName';

    protected static $defaultName = 'app:delete:synapse';

    protected function configure(): void
    {
        $this
            ->setDescription('Deploy Synapse server and other resources');

        $this
            ->addOption(
                self::OPTION_SYNAPSE_DW_SERVER_NAME,
                null,
                InputOption::VALUE_REQUIRED
            );
        $this
            ->addOption(
                self::OPTION_SYNAPSE_SQL_SERVER_NAME,
                null,
                InputOption::VALUE_REQUIRED
            );
        $this
            ->addOption(
                self::OPTION_FILE_STORAGE_RESOURCE_ID,
                null,
                InputOption::VALUE_REQUIRED
            );
        $this
            ->addOption(
                self::OPTION_SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID,
                null,
                InputOption::VALUE_REQUIRED
            );
        parent::configure();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $cli = $this->getCli($input);
        $fileStorageResourceId = $input->getOption(self::OPTION_FILE_STORAGE_RESOURCE_ID);
        $this->assertOption(self::OPTION_FILE_STORAGE_RESOURCE_ID, $fileStorageResourceId);
        $synapseDwServerName = $input->getOption(self::OPTION_SYNAPSE_DW_SERVER_NAME);
        $this->assertOption(self::OPTION_SYNAPSE_DW_SERVER_NAME, $fileStorageResourceId);
        $synapseSqlServerName = $input->getOption(self::OPTION_SYNAPSE_SQL_SERVER_NAME);
        $this->assertOption(self::OPTION_SYNAPSE_SQL_SERVER_NAME, $fileStorageResourceId);
        $resourceGroup = $input->getOption(self::OPTION_AZURE_RESOURCE_GROUP);
        $this->assertOption(self::OPTION_AZURE_RESOURCE_GROUP, $resourceGroup);
        $servicePrincipalObjetId = $input->getOption(self::OPTION_SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID);
        $this->assertOption(self::OPTION_SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID, $servicePrincipalObjetId);

        $output->writeln([
            'Delete managed identity role',
        ]);
        $cli->runAuthorizedCmd(<<< EOT
az role assignment delete \
  --assignee $servicePrincipalObjetId \
  --role "Storage Blob Data Contributor" \
  --scope $fileStorageResourceId
EOT
        );

        $output->writeln([
            'Managed identity role removed',
        ]);

        $output->writeln([
            'Delete Synapse pool',
        ]);
        $cli->runAuthorizedCmd(<<< EOT
        az sql dw delete -y \
    --resource-group $resourceGroup \
    --name $synapseDwServerName \
    --server $synapseSqlServerName
EOT
        );

        $output->writeln([
            'Synapse pool removed',
        ]);

        $output->writeln([
            'Delete Synapse logical sql server',
        ]);
        $cli->runAuthorizedCmd(<<< EOT
az sql server delete -y \
  --resource-group $resourceGroup \
  --name $synapseSqlServerName
EOT
        );
        $output->writeln([
            'Synapse logical sql server removed',
        ]);

        return 0;
    }
}
