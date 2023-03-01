# syntax=docker/dockerfile:1.3

ARG PHP_VERSION=8.1

FROM php:${PHP_VERSION}-cli-buster AS base

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_ACCESS_KEY_ID
ARG AWS_SESSION_TOKEN
WORKDIR /code/

COPY docker/php/php.ini /usr/local/etc/php/php.ini
COPY docker/php/xdebug.ini /usr/local/etc/php/conf.d/

RUN apt-get update -q \
    && apt-get install gnupg -y --no-install-recommends \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list  \
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

RUN pecl install xdebug-3.1.6 \
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
ARG SNOWFLAKE_ODBC_VERSION=2.25.6
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

RUN /usr/bin/aws s3 cp s3://keboola-drivers/teradata/tdodbc1710-17.10.00.08-1.x86_64.deb /tmp/teradata/tdodbc.deb
RUN /usr/bin/aws s3 cp s3://keboola-drivers/teradata/utils/TeradataToolsAndUtilitiesBase__ubuntu_x8664.17.00.34.00.tar.gz  /tmp/teradata/tdutils.tar.gz
RUN /usr/bin/aws s3 cp s3://keboola-drivers/exasol/EXASOL_ODBC-7.1.10.tar.gz /tmp/exasol/odbc.tar.gz

## Teradata
COPY docker/teradata/odbc.ini /tmp/teradata/odbc_td.ini
COPY docker/teradata/odbcinst.ini /tmp/teradata/odbcinst_td.ini

RUN dpkg -i /tmp/teradata/tdodbc.deb \
    && cat /tmp/teradata/odbc_td.ini >> /etc/odbc.ini \
    && cat /tmp/teradata/odbcinst_td.ini >> /etc/odbcinst.ini \
    && docker-php-ext-configure pdo_odbc --with-pdo-odbc=unixODBC,/usr \
    && docker-php-ext-install pdo_odbc \
    && docker-php-source delete

ENV ODBCHOME = /opt/teradata/client/ODBC_64/
ENV ODBCINI = /opt/teradata/client/ODBC_64/odbc.ini
ENV ODBCINST = /opt/teradata/client/ODBC_64/odbcinst.ini
ENV LD_LIBRARY_PATH = /opt/teradata/client/ODBC_64/lib

# Teradata Utils
#COPY --from=td /tmp/teradata/tdutils.tar.gz /tmp/teradata/tdutils.tar.gz
RUN cd /tmp/teradata \
    && tar -xvaf tdutils.tar.gz \
    && sh /tmp/teradata/TeradataToolsAndUtilitiesBase/.setup.sh tptbase s3axsmod azureaxsmod \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/teradata

#Exasol
RUN set -ex; \
    mkdir -p /tmp/exasol/odbc /opt/exasol ;\
    tar -xzf /tmp/exasol/odbc.tar.gz -C /tmp/exasol/odbc --strip-components 1; \
    cp /tmp/exasol/odbc/lib/linux/x86_64/libexaodbc-uo2214lv2.so /opt/exasol/;\
    echo "\n[exasol]\nDriver=/opt/exasol/libexaodbc-uo2214lv2.so\n" >> /etc/odbcinst.ini;\
    rm -rf /tmp/exasol;

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
    https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protoc-21.12-linux-x86_64.zip && \
    unzip /tmp/protoc/protoc.zip -d /tmp/protoc && \
    mv /tmp/protoc/bin/protoc /usr/local/bin && \
    mv /tmp/protoc/include/google /usr/local/include && \
    chmod +x /usr/local/bin/protoc && \
    rm -rf /tmp/protoc

RUN curl -sSLf \
        -o /usr/local/bin/install-php-extensions \
        https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions && \
    chmod +x /usr/local/bin/install-php-extensions
