FROM php:8.3-cli

# Pinned to the ext-mongodb version used by our production PHP images: the Core `devops` repo,
# docker-images/frankenphp/Dockerfile (`install-php-extensions mongodb-2.3.3`, pinned to match base-php8).
ARG EXT_MONGODB_VERSION=2.3.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl-dev pkg-config libicu-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pecl install mongodb-${EXT_MONGODB_VERSION} \
    && docker-php-ext-enable mongodb

WORKDIR /app
COPY smoke.php /app/smoke.php
CMD ["php", "/app/smoke.php"]
