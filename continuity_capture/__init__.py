"""continuity_capture — Kernpaket des M-2 Capture Plugins (bibliothekarische Logik).

Der Hermes-Plugin-Wrapper (Ordnername `continuity-capture/`) importiert dieses Paket
und reicht die Hook-Aufrufe an die CaptureController-API weiter. Aktivierung erfolgt
AUSSCHLIESSLICH durch den Auftraggeber via plugins.enabled — nicht automatisiert hier.
"""

from .config import CaptureConfig, load_config, resolve_agent_id, resolve_spool_path
from .registry import RegistryRejection, assert_registered, is_registered, active_registry
from .spool import Spool
from .sender import BridgeSink, SenderThread
from .normalizer import NormalizeResult, normalize
from .identity import ProcessReference, default_host_identity, make_instance_id
from .nats_client import NatsClient, NatsError
from .nats_transport import JetStream, NatsTransport, TransportOutcome, build_jetstream_transport

__all__ = [
    "CaptureConfig", "load_config", "resolve_agent_id", "resolve_spool_path",
    "RegistryRejection", "assert_registered", "is_registered", "active_registry",
    "Spool", "BridgeSink", "SenderThread",
    "NormalizeResult", "normalize",
    "ProcessReference", "default_host_identity", "make_instance_id",
    "NatsClient", "NatsError",
    "JetStream", "NatsTransport", "TransportOutcome", "build_jetstream_transport",
]
__version__ = "0.2.2"
