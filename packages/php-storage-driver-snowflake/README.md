# Keboola Storage Driver Snowflake

Keboola high level storage backend driver for Snowflake.


### .env

.env.dist > .env + doplnit secrety

### Snowflake

Prepare credentials for Snowflake access
Create RSA key pair for Snowflake user, you can use the following command to generate it:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Then you can use the public key in the Snowflake user creation script below.

```snowflake
CREATE ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
CREATE DATABASE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";

GRANT ALL PRIVILEGES ON DATABASE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";

CREATE USER "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE"
PASSWORD = ''
DEFAULT_ROLE = "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE"
RSA_PUBLIC_KEY = '<your_public_key>'
;

GRANT ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" TO USER "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
```

set up env variables:

```dotenv
SNOWFLAKE_HOST: keboolaconnectiondev.us-east-1.snowflakecomputing.com
SNOWFLAKE_PORT: 443
SNOWFLAKE_USER: KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE
SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }} # note: it has to be full private key in PEM format, including the header and footer
SNOWFLAKE_DATABASE: KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE
SNOWFLAKE_WAREHOUSE: DEV
```
