FROM php:7.4-cli
MAINTAINER Ondrej Hlavacek <ondrej.hlavacek@keboola.com>

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

RUN apt-get update \
  && apt-get install unzip git -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

WORKDIR /code/

COPY docker/composer-install.sh /tmp/composer-install.sh
RUN chmod +x /tmp/composer-install.sh \
    && /tmp/composer-install.sh

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS
