<?php

declare(strict_types=1);

namespace Keboola\Provisioning;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class Deploy extends BaseCmd
{
    private const OPTION_AZURE_RESOURCE_GROUP = 'resourceGroup';
    private const OPTION_SERVER_NAME = 'serverName';

    protected static string $defaultName = 'app:deploy:synapse';

    protected function configure(): void
    {
        $this
            ->setDescription('Deploy Synapse server and other resources');

        $this
            ->addOption(
                self::OPTION_SERVER_NAME,
                null,
                InputOption::VALUE_REQUIRED,
            );
        $this
            ->addOption(
                self::OPTION_AZURE_RESOURCE_GROUP,
                null,
                InputOption::VALUE_REQUIRED,
            );
        parent::configure();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $cli = $this->getCli($input);
        $env = new EnvCollector();
        $serverName = $input->getOption(self::OPTION_SERVER_NAME);
        $this->assertOption(self::OPTION_SERVER_NAME, $serverName);
        $resourceGroup = $input->getOption(self::OPTION_AZURE_RESOURCE_GROUP);
        $this->assertOption(self::OPTION_AZURE_RESOURCE_GROUP, $resourceGroup);

        $serverPassword = $this->runCmdSingleLineOutput('openssl rand -base64 32');
        $suffix = $this->runCmdSingleLineOutput('openssl rand -hex 5');
        $blobDeploymentName = sprintf('ABS_%s_%s', $serverName, $suffix);
        $synapseDeploymentName = sprintf('Synapse_%s_%s', $serverName, $suffix);

        $output->writeln([
            'Start blob storage deploy',
        ]);
        $outBlobDeploy = $cli->runAuthorizedCmd(<<< EOT
az group deployment create \
  --name $blobDeploymentName \
  --resource-group $resourceGroup \
  --template-file /keboola/provisioning/ABS/template.json \
  --output json \
  --parameters \
    suffix=$suffix
EOT,);
        $outBlobDeploy = $this->getDeploymentResult($outBlobDeploy);
        $storageAccountName = $outBlobDeploy['properties']['outputs']['storageAccountName']['value'];
        $storageAccountKey = $outBlobDeploy['properties']['outputs']['storageAccountKey']['value'];

        $output->writeln([
            'Blob storage deployed',
            'ABS_ACCOUNT_NAME: ' . $storageAccountName,
            'ABS_ACCOUNT_KEY: ' . $storageAccountKey,
        ]);
        $env->addEnv('ABS_ACCOUNT_NAME', $storageAccountName);
        $env->addEnv('ABS_ACCOUNT_KEY', $storageAccountKey);

        $output->writeln([
            'Start Synapse server deploy',
        ]);
        $outSynapseDeploy = $cli->runAuthorizedCmd(<<< EOT
az group deployment create \
  --name $synapseDeploymentName \
  --resource-group $resourceGroup \
  --template-file /keboola/provisioning/synapse/synapse.json \
  --output json \
  --parameters \
    administratorLogin=keboola \
    administratorPassword=$serverPassword \
    warehouseName=$serverName \
    suffix=$suffix \
    warehouseCapacity=900
EOT,);

        $outSynapseDeploy = $this->getDeploymentResult($outSynapseDeploy);
        $synapseSqlServerName = $outSynapseDeploy['properties']['outputs']['sqlServerName']['value'];
        $synapseDwServerName = $outSynapseDeploy['properties']['outputs']['warehouseName']['value'];
        $synapseResourceId = $outSynapseDeploy['properties']['outputs']['warehouseResourceId']['value'];

        $output->writeln([
            'Synapse server deployed',
            'SYNAPSE_SQL_SERVER_NAME: ' . $synapseSqlServerName,
            'SYNAPSE_DATABASE: ' . $synapseDwServerName,
            'SYNAPSE_RESOURCE_ID: ' . $synapseResourceId,
            'SYNAPSE_PWD: ' . $serverPassword,
        ]);
        $env->addEnv('SYNAPSE_UID', 'keboola');
        $env->addEnv('SYNAPSE_PWD', $serverPassword);
        $env->addEnv('SYNAPSE_DATABASE', $synapseDwServerName);
        $env->addEnv('SYNAPSE_DW_SERVER_NAME', $synapseDwServerName);
        $env->addEnv('SYNAPSE_SERVER', sprintf('%s.database.windows.net', $synapseSqlServerName));
        $env->addEnv('SYNAPSE_SQL_SERVER_NAME', $synapseSqlServerName);
        $env->addEnv('SYNAPSE_RESOURCE_ID', $synapseResourceId);

        $output->writeln([
            'Start deploy firewall rule',
        ]);

        $cli->runAuthorizedCmd(<<< EOT
az sql server firewall-rule create \
  --resource-group $resourceGroup \
  --server $synapseSqlServerName \
  --name all \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 255.255.255.255
EOT,);

        $output->writeln([
            'Firewall rule set done',
        ]);

        // managed identity
        $principalObjectId = $cli->runAuthorizedSingleLineOutputCmd(<<< EOT
    az ad sp list \
      --display-name $synapseSqlServerName \
      --query "[].objectId" \
      --output tsv
EOT,);

        $output->writeln([
            'SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID: ' . $principalObjectId,
        ]);

        $fileStorageResourceId = $cli->runAuthorizedSingleLineOutputCmd(<<< EOT
az resource show \
      -g $resourceGroup \
      --resource-type "Microsoft.Storage/storageAccounts" \
      -n $storageAccountName \
      --query "id" \
      --output tsv
EOT,);

        $output->writeln([
            'FILE_STORAGE_RESOURCE_ID: ' . $fileStorageResourceId,
        ]);

        $cli->runAuthorizedCmd(<<< EOT
az role assignment create \
      --assignee $principalObjectId \
      --role "Storage Blob Data Contributor" \
      --scope $fileStorageResourceId
EOT,);

        $output->writeln([
            'Managed identity role created',
        ]);

        $env->addEnv('SYNAPSE_SERVICE_PRINCIPAL_OBJECT_ID', $principalObjectId);
        $env->addEnv('FILE_STORAGE_RESOURCE_ID', $fileStorageResourceId);

        return 0;
    }

    /**
     * @return array<mixed>
     */
    protected function getDeploymentResult(string $outDeployment): array
    {
        $outDeployment = trim(preg_replace('/\s+/', ' ', $outDeployment));
        return json_decode($outDeployment, true);
    }
}
