# Keboola Storage Driver Common Guidelines

## Errors

* All Exceptions from driver must implement `Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface`
* Or !preferred! extend `Keboola\StorageDriver\Shared\Driver\Exception\Exception`

### Exceptions are distinguished by integer codes

    - 0 is default for php exception and kept as unknow exception
    - 900-999 Driver implementation exceptions 
    - 1000-1099 Connection related exception codes 
    - 2xxx Objects related exception codes
    - 3xxx Validation exception codes
    - 4xxx Import exceptions
    - 5xxx Export exceptions
    - 6xxx SQL exceptions

### Exceptions rules 

- Driver can add own codes but must keep them in above specified ranges.
- Different exception codes from specified in ExceptionInterface will be always threaded in Keboola Connection as Application errors
- Keboola Connection will throw User exception only for certain codes within certain commands executed
- Do not throw `Keboola\CommonExceptions\UserExceptionInterface` inside driver!
