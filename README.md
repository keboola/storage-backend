# storage-backend
Storage backend related tools monorepo

## Packages included in this repo:

| Package name                                                     | Description                                                                                                     |
|-------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| [keboola/php-datatypes](https://github.com/keboola/php-datatypes) | datatype definitions, validation and sql statement generation                                                   |
| [keboola/php-table-backend-utils](https://github.com/keboola/php-table-backend-utils) | Common stuff for table backends (Snowflake, Synapse, Redshift, Exasol, Teradata, Bigquery) shared between apps. |


## Adding new repository
If you want to add a new repository to monorep, please follow the [instructions](adopt-new-repository.md).

## Provisioning of resources
Some libraries need to install backend drivers that are stored on S3 for this purpose we create a [cloudformation template](provisioning/AWS/aws-resources.yaml).
This cloudformation template is managed manually on the corresponding AWS stack for CI. When adding a new library to the monorepo, we can use the base role for building and use
as in [main.yml](.github/workflows/main.yml) on line 32, or add the new role to the cloudformation template and assign the necessary resources to it.
```yaml
      -
        name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v1
        with:
          role-to-assume: ${{ env.AWS_CREDENTIALS_ROLE_TO_ASSUME }}
          aws-region: ${{ env.AWS_CREDENTIALS_REGION }}
```

## How to use monorepo and packages
We maintaining the monorepo with the [symplify/monorepo-builder](https://github.com/symplify/monorepo-builder).

The base docker service is `dev`
To install dependencies use: 

```bash
docker-compose build dev
docker-compose run dev composer install

docker-compose run dev vendor/bin/monorepo-builder
```

The individual package services in the monorepo are then prefixed with the word `dev-`.
So if we want to install composer for example for the `php-table-backend-utils` package we use the command:

```bash
docker-compose build dev-php-table-backend-utils
docker-compose run dev-php-table-backend-utils composer install
```

and run for example unit tests like this:

```bash
docker-compose run dev-php-table-backend-utils composer tests-unit
```

By separating the packages and installing each of them separately when we run the previous command.
Because the other packages in monorep are installed from [packagist.org](https://packagist.org/),
so individual customizations are not shared between packages.

For example, `php-table-backend-utils` required on `php-datypes`, but if I make a change to `php-datatypes` and 
call `docker-compose run dev-php-table-backend-utils composer tests-unit` the changes will not be reflected in the `php-table-backend-utils` tests.


## Running tests locally

To be able to run tests to include all changes and packages requested via composer we can use global phpunit.

`docker-compose run dev vendor/bin/phpunit packages/php-table-backend-utils/tests/Unit/`

However, by creating a new CLI interpreter in PHPStorm we will achieve that the changes will be reflected in the whole monorepo.

So for each lib we will create our own interpreter.

For example for lib `php-table-backend-utils` as seen in the picture.

![Cli interpreter in phpstorm](cli-interpreter-table-utils.png?raw=true "Cli interpreter in phpstorm")