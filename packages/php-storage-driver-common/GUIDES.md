# Keboola Storage Driver Common Guidelines

## Errors

* All Exceptions from the driver must implement `Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface`
* Preferably, extend `Keboola\StorageDriver\Shared\Driver\Exception\Exception`
* 
### Exceptions are distinguished by integer codes

- 0 is the default for PHP exceptions and kept as an unknown exception
- 900-999 are Driver implementation exceptions
- 1000-1099 are Connection related exception codes
- 2xxx are Objects related exception codes
- 3xxx are Validation exception codes
- 4xxx are Import exceptions
- 5xxx are Export exceptions
- 6xxx are SQL exceptions

### Exceptions rules

- The Driver can add its own codes but must keep them within the specified ranges.
- Any exceptions not specified in the ExceptionInterface will always be treated in Keboola Connection as Application errors
- Keboola Connection will only throw User exceptions for certain codes within certain commands executed
- Do not throw `Keboola\CommonExceptions\UserExceptionInterface` inside the Driver!

### Exception retry

Keboola Connection will do a retry if something on the backend fails, but some exceptions are idempotent and doing a retry is only wasting time and resources. The Driver can suggest to Keboola Connection which exceptions should and should not be retried.

By implementing the method `Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface::isRetryable` the Driver can decide if an exception is idempotent and should not be retried.
The best practice is to implement your own exception extending `Keboola\StorageDriver\Shared\Driver\Exception\Exception`:
- Without change, the exception will be set to always retry
- To suggest that the exception cannot be retried, also implement `Keboola\StorageDriver\Contract\Driver\Exception\NonRetryableExceptionInterface`
