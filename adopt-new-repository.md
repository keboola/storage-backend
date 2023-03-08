1. build docker container `docker build -t monorepo-tools ./bin`
2. run container and connect to it `docker run -it -v $(pwd):/monorepo -w /monorepo monorepo-tools bash`
3. set git user to Github Actions
```bash
git config --global user.email "git@github.com"
git config --global user.name "Github Actions"
```
4. set the name of the repository you want to add to monorepo
```bash
export REPO=<name-of-repository>
```
5. run adopt repo command, this clones the repository into the monorepo folder
```bash
bin/adopt-repo.sh https://github.com/keboola/$REPO.git packages/$REPO $REPO/
```
6. check the changes in the git and if we are satisfied and the repository has been added correctly with all commits we move on
7. Add github action CI workflows from old repo to .github in root and set envs for tests, according to how other repositories are added
8. For monorepo split, go to the destination repository and create new deploy key (under `Settings → Deploy Keys`).
   Generate the key using 1Password and store it directly in the shared vault.
   Don't forget to give it write access! Store that key in this repository [Action Secrets](https://github.com/keboola/storage-backend/settings/secrets/actions) under `[REPO_NAME]_SSH_PRIVATE_KEY` and also fill it's name into the matrix in monorepo_split job in `.github/workflows/main.yml`. 
9. Remove branch protection rules on the destination repository to allow the deploy key to push any updates there. The branch setup should look like [this](https://github.com/keboola/php-storage-driver-common/settings/branches)

## Github Action
For automatic testing we use Github Action, all workflows are located in `.github/workflows`.

So when adding a new repository, you need to add a workflow for the new repository as well.

1. add a new file `build-<name-of-lib>.yml` to the workflows folder `.github/workflows`
2. add the required actions to it

### Setup dependencies between packages
Some packages may require different packages in the monorepo, so you need to set the dependencies between them so that when 
change the package that is used in another package, You need to run the tests of both of them.

For example: the `php-table-backend-utils` package requires the `php-datatypes` package.

This binding is defined in `.github/workflow/main.yml` in `steps->paths/paths-filter@v2->with->filters`.

We define a variable `table-utils-requirements` which is true if something in the path `packages/php-datatypes/**` is changed. 
There can be more than one of these paths in case a package uses multiple packages from a monorepo.

Then we just have to define the outputs `table-utils-requirements: ${{ steps.changes.outputs.table-utils-requirements }}`.

main.yml
```yml
  build:
    runs-on: ubuntu-latest
    outputs:
      changed-php-datatypes: ${{ steps.changes.outputs.php-datatypes }}
      changed-php-table-backend-utils: ${{ steps.changes.outputs.php-table-backend-utils }}
      table-utils-requirements: ${{ steps.changes.outputs.table-utils-requirements }}
    steps:
      - uses: actions/checkout@v3
      - uses: dorny/paths-filter@v2
        id: changes
        with:
          filters: |
            php-datatypes:
              - 'packages/php-datatypes/**'
            php-table-backend-utils:
              - 'packages/php-table-backend-utils/**'
            table-utils-requirements:
              - 'packages/php-datatypes/**'
```
This variable is then added to the build call of the given package.

main.yml
```yaml
  build_php_table_backend_utils:
    uses: ./.github/workflows/build-php-table-backend-utils.yml
    with:
      hasCodeChanged: ${{ needs.build.outputs.changed-php-table-backend-utils == 'true' }}
      isTag: ${{ startsWith(github.ref, 'refs/tags/') }}
      isRequiredRepoChanged: ${{ needs.build.outputs.table-utils-requirements== 'true' }}
    needs: build
```
And we'll use it in the build run condition along with the other run conditions.

build-php-table-backend-utils.yml
```yaml
  build_image_php_table_backend_utils:
    if: ${{ inputs.hasCodeChanged || inputs.isTag || (inputs.isRequiredRepoChanged && inputs.isTag)}}
```