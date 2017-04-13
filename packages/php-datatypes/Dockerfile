FROM php:7.0
MAINTAINER Ondrej Hlavacek <ondrej.hlavacek@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
  && apt-get install unzip git -y

RUN echo "memory_limit = -1" >> /usr/local/etc/php/php.ini

RUN yes | pecl install xdebug \
    && echo "zend_extension=$(find /usr/local/lib/php/extensions/ -name xdebug.so)" > /usr/local/etc/php/conf.d/xdebug.ini

RUN cd \
  && curl -sS https://getcomposer.org/installer | php \
  && ln -s /root/composer.phar /usr/local/bin/composer

