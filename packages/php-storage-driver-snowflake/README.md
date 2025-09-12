# Keboola Storage Driver Snowflake

Keboola high level storage backend driver for Snowflake.


### .env

```bash
cp .env.dist .env
# and fill in the required values
```

### Snowflake

Prepare credentials for Snowflake access
Create RSA key pair for Snowflake user, you can use the following command to generate it:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Then you can use the public key in the Snowflake user creation script below.

To get the public key in one line (without header and footer) you can use:
```bash
PUBLIC_KEY=$(sed '1d;$d' rsa_key.pub | tr -d '\n')
# add this to CREATE USER statement
echo "RSA_PUBLIC_KEY='${PUBLIC_KEY}'"
```

```snowflake
CREATE ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
CREATE DATABASE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
GRANT CREATE DATABASE ON ACCOUNT TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
GRANT CREATE ROLE ON ACCOUNT TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" WITH GRANT OPTION;
GRANT CREATE USER ON ACCOUNT TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" WITH GRANT OPTION;

GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" WITH GRANT OPTION;

CREATE USER "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE"
    PASSWORD = '' --create some password
    TYPE=LEGACY_SERVICE
DEFAULT_ROLE = "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE"
RSA_PUBLIC_KEY = '<your_public_key>'
;

GRANT ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" TO USER "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
GRANT ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON DATABASE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE" TO ROLE "KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE";
```

set up env variables:

For local tests and CI we need to edit the private key to one line and trim `-----BEGIN PRIVATE KEY----- -----END PRIVATE KEY-----` We can do this with `cat rsa_key.p8 | sed '1d;$d' | tr -d '\n'`
```bash
# set private key in your local .env file
PRIVATE_KEY=$(sed '1d;$d' rsa_key.p8 | tr -d '\n'); if grep -q '^SNOWFLAKE_PRIVATE_KEY=' .env; then sed -i "s|^SNOWFLAKE_PRIVATE_KEY=.*|SNOWFLAKE_PRIVATE_KEY=\"$PRIVATE_KEY\"|" .env; else echo "SNOWFLAKE_PRIVATE_KEY=\"$PRIVATE_KEY\"" >> .env; fi
```

```dotenv
SNOWFLAKE_HOST: keboolaconnectiondev.us-east-1.snowflakecomputing.com
SNOWFLAKE_PORT: 443
SNOWFLAKE_USER: KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE
SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }} # note: it has to be full private key in PEM format, including the header and footer
SNOWFLAKE_DATABASE: KEBOOLA_CI_PHP_STORAGE_DRIVER_SNOWFLAKE
SNOWFLAKE_WAREHOUSE: DEV
```
