from __future__ import annotations

from ._client_endpoints_execution import ExecutionEndpoints
from ._client_endpoints_identity import IdentityEndpoints
from ._client_endpoints_intelligence import IntelligenceEndpoints
from ._client_endpoints_operations import OperationsEndpoints


class SwarmClientEndpoints(
    IdentityEndpoints,
    IntelligenceEndpoints,
    ExecutionEndpoints,
    OperationsEndpoints,
):
    """Composes the public endpoint groups without owning transport behavior."""
