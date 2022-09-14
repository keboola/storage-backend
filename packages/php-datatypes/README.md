# Keboola Datatypes
[![Build Status](https://travis-ci.org/keboola/php-datatypes.svg)](https://travis-ci.org/keboola/php-datatypes)
[![Latest Stable Version](https://poser.pugx.org/keboola/php-datatypes/version)](https://packagist.org/packages/keboola/php-datatypes)
[![Code Climate](https://codeclimate.com/github/keboola/php-datatypes/badges/gpa.svg)](https://codeclimate.com/github/keboola/php-datatypes)
[![Test Coverage](https://codeclimate.com/github/keboola/php-datatypes/badges/coverage.svg)](https://codeclimate.com/github/keboola/php-datatypes/coverage)

## Introduction
This library is used for datatype definitions, validation and sql statement generation. 
Each type (Mysql, Redshift, Snowflake, GenericStorage) also provides a base type mapping that can be used to map types between different storage providers.

## Base Types
There are currently 7 base types: `String`, `Date`, `Timestamp`, `Integer`, `Numeric`, `Float`, and `Boolean`.
Any type class that extends `Keboola\Datatype\Definition\Common` should provide a mapping for it's types to the above set.
 This will allow us to map data-types between storage providers. 

## Build

```
git clone https://github.com/keboola/php-datatypes
cd php-datatypes
docker-compose build
docker-compose run --rm dev composer install
```

## Tests Execution
Run tests with following command.

```
docker-compose run --rm dev ./vendor/bin/phpunit
```

## Coding Style

```
docker-compose run --rm dev ./vendor/bin/phpcs --standard=psr2 --ignore=vendor -n .
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
