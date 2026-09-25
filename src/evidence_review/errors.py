"""服务层可观察错误。"""


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = details


class NotFound(ServiceError):
    code = "not_found"
    status = 404


class Conflict(ServiceError):
    code = "conflict"
    status = 409


class ReviewConflict(Conflict):
    """复核竞争失败：申请已存在有效决定，本请求未生效。"""

    code = "review_conflict"


class Forbidden(ServiceError):
    code = "forbidden"
    status = 403


class InvalidState(ServiceError):
    code = "invalid_state"
    status = 409


class ValidationFailed(ServiceError):
    code = "validation_failed"
    status = 422
