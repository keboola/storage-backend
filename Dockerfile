# syntax=docker/dockerfile:1.3

ARG PHP_VERSION=8.1

FROM php:${PHP_VERSION}-cli-buster AS base

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

ARG SQLSRV_VERSION=5.10.1
ARG SNOWFLAKE_ODBC_VERSION=2.25.6
ARG SNOWFLAKE_GPG_KEY=630D9F3CAB551AF3

WORKDIR /code/

COPY docker/php/php.ini /usr/local/etc/php/php.ini
COPY docker/php/xdebug.ini /usr/local/etc/php/conf.d/

RUN apt-get update -q \
    && apt-get install gnupg -y --no-install-recommends \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends\
        git \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        gpg \
        debsig-verify \
        dirmngr \
        gpg-agent \
        libonig-dev \
        libxml2-dev \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
    && docker-php-ext-configure intl \
    && docker-php-ext-install intl

# Snowflake ODBC
# https://github.com/docker-library/php/issues/103#issuecomment-353674490
RUN set -ex; \
    docker-php-source extract; \
    { \
        echo '# https://github.com/docker-library/php/issues/103#issuecomment-353674490'; \
        echo 'AC_DEFUN([PHP_ALWAYS_SHARED],[])dnl'; \
        echo; \
        cat /usr/src/php/ext/odbc/config.m4; \
    } > temp.m4; \
    mv temp.m4 /usr/src/php/ext/odbc/config.m4; \
    docker-php-ext-configure odbc --with-unixODBC=shared,/usr; \
    docker-php-ext-install odbc; \
    docker-php-source delete 

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

RUN pecl install xdebug-3.1.6 \
 && docker-php-ext-enable xdebug

FROM base AS dev
WORKDIR /code

FROM quay.io/keboola/aws-cli
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
RUN /usr/bin/aws s3 cp s3://keboola-drivers/teradata/tdodbc1710-17.10.00.08-1.x86_64.deb /tmp/teradata/tdodbc.deb
RUN /usr/bin/aws s3 cp s3://keboola-drivers/exasol/EXASOL_ODBC-7.1.10.tar.gz /tmp/exasol/odbc.tar.gz

FROM base AS php-table-backend-utils
ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer
ENV LIB_NAME=php-table-backend-utils
ENV LIB_HOME=/code/${LIB_NAME}
WORKDIR ${LIB_HOME}

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

#Synapse ODBC
RUN set -ex; \
    pecl install sqlsrv-$SQLSRV_VERSION pdo_sqlsrv-$SQLSRV_VERSION; \
    docker-php-ext-enable sqlsrv pdo_sqlsrv; \
    docker-php-source delete

## Snowflake
COPY ./docker/snowflake/generic.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY/generic.pol
COPY ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

COPY packages/${LIB_NAME}/composer.json ./
RUN --mount=type=bind,target=/packages,source=packages \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

COPY packages/${LIB_NAME} ./