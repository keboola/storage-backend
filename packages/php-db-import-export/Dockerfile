FROM php:7-cli

ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ARG DEBIAN_FRONTEND=noninteractive
ENV COMPOSER_ALLOW_SUPERUSER 1
ENV COMPOSER_PROCESS_TIMEOUT 3600

WORKDIR /code/

COPY docker/php-prod.ini /usr/local/etc/php/php.ini
COPY docker/composer-install.sh /tmp/composer-install.sh

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        locales \
        unzip \
	&& rm -r /var/lib/apt/lists/* \
	&& sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
	&& locale-gen \
	&& chmod +x /tmp/composer-install.sh \
	&& /tmp/composer-install.sh

ENV LANGUAGE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

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
