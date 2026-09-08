class DalmatianError(Exception):
    pass


class QueryRejected(DalmatianError, ValueError):
    pass


class BusyError(DalmatianError):
    pass


class QueryCancelled(DalmatianError):
    pass


class QueryTimeout(QueryCancelled):
    pass


class CommitFenced(DalmatianError):
    pass


class IdempotencyConflict(DalmatianError):
    pass


class MetadataSchemaError(DalmatianError):
    pass


class RetryableQueryError(DalmatianError):
    pass
