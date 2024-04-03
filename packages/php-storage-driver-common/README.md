# Keboola Storage Driver Common

Keboola high level common storage backend driver

## Setup AWS credentials

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
# bucket is needed for import/export tests
AWS_S3_BUCKET=
AWS_REGION=eu-central-1
```

## Build docker images

```bash
docker compose build
```

## Xdebug

To run with xdebug use `dev-xdebug` container instead of `dev`

## Generate protobuf code

```bash
docker compose run --rm dev composer protobuf
```

### Generate protobuf documentation

Documentation will be placed in `docs` folder. Check [documentation](https://github.com/pseudomuto/protoc-gen-doc/blob/master/README.md) for more options.
```bash
docker run --rm \
  -v $(pwd)/docs:/out \
  -v $(pwd)/proto:/protos \
  pseudomuto/protoc-gen-doc
```

## Tests

Run tests with following command.

```bash
docker compose run --rm dev composer tests
```

To run separate tests first generate testing protobuf classes
```bash
docker compose run --rm dev composer protobufTests
```

To disable retry copy `phpunit-retry.xml.dist`
```bash
cp phpunit-retry.xml.dist phpunit-retry.xml
```

### Debug queries

To debug queries executed during test set DEBUG=1 env variable 

## Code quality check

```bash
#run all bellow but not tests
docker compose run --rm dev composer check

#phplint
docker compose run --rm dev composer phplint

#phpcs
docker compose run --rm dev composer phpcs

#phpcbf
docker compose run --rm dev composer phpcbf

#phpstan
docker compose run --rm dev composer phpstan
```

## Full CI workflow

This command will run all checks and run tests
```bash
docker compose run --rm dev composer ci
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
