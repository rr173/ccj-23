"""Exception types for the durability system."""


class DurabilityError(Exception):
    """Base class for durability system errors."""


class PlacementBlocked(DurabilityError):
    """Raised when a placement plan cannot be formed.

    Carries human-readable blocking reasons (capacity, fault domains,
    draining nodes, ...).  No partial plan or reservation is ever left
    behind when this is raised.
    """

    def __init__(self, reasons):
        self.reasons = list(reasons)
        super().__init__("; ".join(self.reasons))


class UnreadableError(DurabilityError):
    """Raised when a media version has no verified replica to serve a read."""


class SimulatedCrash(Exception):
    """Raised by test hooks to simulate an abrupt process crash."""
