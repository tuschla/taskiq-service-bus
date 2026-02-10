from importlib.metadata import version

from .broker import ServiceBusBroker

__version__ = version("taskiq-service-bus")
__all__ = ("ServiceBusBroker",)
