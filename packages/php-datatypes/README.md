# Keboola Datatypes
[![Build Status](https://travis-ci.org/keboola/php-datatypes.svg)](https://travis-ci.org/keboola/php-datatypes)
[![Latest Stable Version](https://poser.pugx.org/keboola/php-datatypes/version)](https://packagist.org/packages/keboola/php-datatypes)
[![Code Climate](https://codeclimate.com/github/keboola/php-datatypes/badges/gpa.svg)](https://codeclimate.com/github/keboola/php-datatypes)
[![Test Coverage](https://codeclimate.com/github/keboola/php-datatypes/badges/coverage.svg)](https://codeclimate.com/github/keboola/php-datatypes/coverage)

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
