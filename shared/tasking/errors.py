class TaskingError(Exception):
    """Base class for task runtime errors."""


class UnknownTaskRoutingError(TaskingError):
    """Raised when no queue mapping exists for a task name."""


class InvalidTaskRouteError(TaskingError):
    """Raised when a task is enqueued into the wrong queue."""


class UnknownTaskError(TaskingError):
    """Raised when no task handler is registered for a task name."""


class WorkerTaskNotAllowedError(TaskingError):
    """Raised when a worker receives a task outside its allowed task set."""


class InvalidTaskPayloadError(TaskingError):
    """Raised when a task payload does not match its declared schema."""
