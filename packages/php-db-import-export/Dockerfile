ARG PHP_VERSION=8.3.19

FROM php:${PHP_VERSION:-8.3.19}-cli-bullseye
MAINTAINER Keboola <devel@keboola.com>

ARG GITHUB_OAUTH_TOKEN
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 7200

ARG SQLSRV_VERSION=5.10.1
ARG SNOWFLAKE_ODBC_VERSION=3.10.0
ARG SNOWFLAKE_GPG_KEY=2A3149C82551A34A
ARG GOOGLE_CLOUD_CLI_VERSION=393.0.0

ENV LC_CTYPE=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV LANGUAGE=C.UTF-8

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

# has to be done first, because MS has thier versions of following packages
RUN apt-get update -q \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends\
        unixodbc \
        unixodbc-dev
RUN apt-get install gnupg -y --no-install-recommends \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list  \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -  \
    && apt-get update -q \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends\
        git \
        apt-transport-https \
        ca-certificates \
        unzip \
        libpq-dev \
        gpg \
        debsig-verify \
        dirmngr \
        gpg-agent \
        libonig-dev \
        libxml2-dev \
        parallel \
        google-cloud-cli \
    && rm -r /var/lib/apt/lists/* \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh


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


#php odbc
RUN docker-php-ext-configure pdo_odbc --with-pdo-odbc=unixODBC,/usr \
    && docker-php-ext-install pdo_odbc

#other php extensions
RUN docker-php-ext-install bcmath

## Composer - deps always cached unless changed
# First copy only composer files
COPY composer.* /code/
# Download dependencies, but don't run scripts or init autoloaders as the app is missing
RUN composer install $COMPOSER_FLAGS --no-scripts --no-autoloader
# copy rest of the app
COPY . /code/
# run normal composer - all deps are cached already
RUN composer install $COMPOSER_FLAGS

CMD ["php", "/code/src/run.php"]
