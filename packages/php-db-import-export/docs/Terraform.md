# Setup cloud resources for development

## Prerequisites:

- configured az, aws and gcp CLI tools
   - [AWS CLI](https://keboola.atlassian.net/wiki/spaces/KB/pages/2559475718/AWS+CLI#Using-named-profiles)
- logged in CLI tools
   - `az login`
     - `az account set --subscription eac4eb61-1abe-47e2-a0a1-f0a7e066f385` (Keboola DEV Connection Team)
     - PIMp my role: https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3251437579/Common+Repository+Setup#Azure 
   - `aws sso login --profile=Keboola-Dev-Connection-Team-AWSAdministratorAccess`
     - this assumes you have the profile set up in `~/.aws/config` file
     - 
       ```
       [profile Keboola-Dev-Connection-Team-AWSAdministratorAccess]
       sso_start_url = https://keboola.awsapps.com/start#/
       sso_region = us-east-1
       sso_account_id = 532553470754
       sso_role_name = AWSAdministratorAccess
       region = eu-central-1
       output = json
       ```
   - `gcloud auth application-default login`
- installed terraform (https://www.terraform.io) and jq (https://stedolan.github.io/jq) to setup local env
- installed docker to run & develop the app

> If you would like to use `$AWS_PROFILE` environment variable, then you need to unset `$AWS_ACCESS_KEY_ID` and `$AWS_KEY_SECRET` environment variables. Otherwise `aws cli` will use them instead of `$AWS_PROFILE`.

## Setting up AWS resources

```shell
# set environment variables
export AWS_PROFILE=Keboola-Dev-Connection-Team-AWSAdministratorAccess
export NAME_PREFIX=<your_nick> # your name/nickname to make your resource unique & recognizable

# persist your prefix
cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

# Initialize and apply terraform configuration
terraform -chdir=./provisioning/local init
terraform -chdir=./provisioning/local apply

# Update environment variables for AWS
./provisioning/local/update-env.sh aws
```

> **Important**: Be careful and double check ENVs in .env.local file are not duplicated. The script adds environment variables between special delimiter comments.
