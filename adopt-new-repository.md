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
8. For monorepo split, go to the destination repository and create new deploy key (under `Settings → Deploy Keys`). Don't forget to give it write access! Store that key in this repository [Action Secrets](https://github.com/keboola/storage-backend/settings/secrets/actions) under `[REPO_NAME]_SSH_PRIVATE_KEY` and also fill it's name into the matrix in monorepo_split job in `.github/workflows/main.yml`. 
