# syntax=docker/dockerfile:1.4.0

ARG PHP_VERSION=8.3.19

FROM php:${PHP_VERSION}-cli-bullseye AS base

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 4500
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
WORKDIR /code/

COPY docker/php/php.ini /usr/local/etc/php/php.ini
COPY docker/php/xdebug.ini /usr/local/etc/php/conf.d/xdebug.ini

RUN apt-get update -q \
    && apt-get install gnupg -y --no-install-recommends \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list  \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -  \
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends\
        git \
        apt-transport-https \
        ca-certificates \
        locales \
        unzip \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        gpg \
        debsig-verify \
        dirmngr \
        gpg-agent \
        msodbcsql17 \
        libonig-dev \
        libxml2-dev \
        awscli \
        parallel \
        google-cloud-cli \
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

RUN pecl install xdebug \
 && docker-php-ext-enable xdebug

COPY composer.json ./

COPY src src/
COPY monorepo-builder.php .

RUN composer install $COMPOSER_FLAGS

ARG COMPOSER_MIRROR_PATH_REPOS=1
ARG COMPOSER_HOME=/tmp/composer

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

ARG SQLSRV_VERSION=5.10.1
ARG SNOWFLAKE_ODBC_VERSION=2.25.12
ARG SNOWFLAKE_GPG_KEY=630D9F3CAB551AF3

#Synapse ODBC
RUN set -ex; \
    pecl install sqlsrv-$SQLSRV_VERSION pdo_sqlsrv-$SQLSRV_VERSION; \
    docker-php-ext-enable sqlsrv pdo_sqlsrv; \
    docker-php-source delete

## Snowflake
COPY ./docker/snowflake/generic.pol /etc/debsig/policies/$SNOWFLAKE_GPG_KEY/generic.pol
COPY ./docker/snowflake/simba.snowflake.ini /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

RUN mkdir -p ~/.gnupg \
    && chmod 700 ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && mkdir -p /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY \
    && gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys $SNOWFLAKE_GPG_KEY \
    && gpg --export $SNOWFLAKE_GPG_KEY > /usr/share/debsig/keyrings/$SNOWFLAKE_GPG_KEY/debsig.gpg \
    && curl https://sfc-repo.snowflakecomputing.com/odbc/linux/$SNOWFLAKE_ODBC_VERSION/snowflake-odbc-$SNOWFLAKE_ODBC_VERSION.x86_64.deb --output /tmp/snowflake-odbc.deb \
    && debsig-verify /tmp/snowflake-odbc.deb \
    && gpg --batch --delete-key --yes $SNOWFLAKE_GPG_KEY \
    && dpkg -i /tmp/snowflake-odbc.deb


FROM base AS dev
WORKDIR /code

FROM base AS php-table-backend-utils

ENV LIB_NAME=php-table-backend-utils
ENV LIB_HOME=/code/packages/${LIB_NAME}

COPY packages ./packages
WORKDIR ${LIB_HOME}
COPY packages/${LIB_NAME}/composer.json ${LIB_HOME}/
RUN --mount=type=bind,target=/packages,source=packages \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

FROM base AS php-db-import-export

ENV LIB_NAME=php-db-import-export
ENV LIB_HOME=/code/packages/${LIB_NAME}

COPY packages ./packages
WORKDIR ${LIB_HOME}
COPY packages/${LIB_NAME}/composer.json ${LIB_HOME}/
RUN --mount=type=bind,target=/packages,source=packages \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

FROM base AS php-storage-driver-common

ENV LIB_NAME=php-storage-driver-common
ENV LIB_HOME=/code/packages/${LIB_NAME}

COPY packages ./packages
WORKDIR ${LIB_HOME}
COPY packages/${LIB_NAME}/composer.json ${LIB_HOME}/
RUN --mount=type=bind,target=/packages,source=packages \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS

RUN mkdir -p /tmp/protoc && \
    curl -sSLf \
    -o /tmp/protoc/protoc.zip \
    https://github.com/protocolbuffers/protobuf/releases/download/v23.4/protoc-23.4-linux-x86_64.zip && \
    unzip /tmp/protoc/protoc.zip -d /tmp/protoc && \
    mv /tmp/protoc/bin/protoc /usr/local/bin && \
    mv /tmp/protoc/include/google /usr/local/include && \
    chmod +x /usr/local/bin/protoc && \
    rm -rf /tmp/protoc

RUN curl -sSLf \
        -o /usr/local/bin/install-php-extensions \
        https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions && \
    chmod +x /usr/local/bin/install-php-extensions

# php-storage-driver-snowflake
FROM base AS php-storage-driver-snowflake

ENV LIB_NAME=php-storage-driver-snowflake
ENV LIB_HOME=/code/packages/${LIB_NAME}

COPY packages ./packages
WORKDIR ${LIB_HOME}
COPY packages/${LIB_NAME}/composer.json ${LIB_HOME}/
RUN --mount=type=bind,target=/packages,source=packages \
    --mount=type=cache,id=composer,target=${COMPOSER_HOME} \
    composer install $COMPOSER_FLAGS
