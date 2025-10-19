"""AzureLogAnalytics Authentication."""

from __future__ import annotations

import sys
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.core.pipeline.policies import AzureKeyCredentialPolicy

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class AzureLogAnalyticsAuthenticator:
    """Authenticator class for AzureLogAnalytics using DefaultAzureCredential or AzureKeyCredential for testing."""

    def __init__(self, workspace_id: str | None = None) -> None:
        """Initialize the authenticator with appropriate credential based on workspace ID.
        
        Args:
            workspace_id: The Azure Log Analytics workspace ID to determine authentication method.
        """
        self._credential: DefaultAzureCredential | AzureKeyCredential | None = None
        self._authentication_policy: AzureKeyCredentialPolicy | None = None
        self._workspace_id = workspace_id

    @property
    def credential(self) -> DefaultAzureCredential | AzureKeyCredential:
        """Get the Azure credential instance.
        
        Returns:
            DefaultAzureCredential or AzureKeyCredential instance for authentication.
        """
        if self._credential is None:
            # Check if we're using the demo workspace for testing
            if self._workspace_id == "DEMO_WORKSPACE":
                self._credential = AzureKeyCredential("DEMO_KEY")
            else:
                self._credential = DefaultAzureCredential()
        return self._credential

    @property
    def authentication_policy(self) -> AzureKeyCredentialPolicy | None:
        """Get the authentication policy for the credential.
        
        Returns:
            AzureKeyCredentialPolicy for demo workspace, None for production.
        """
        if self._authentication_policy is None:
            # Check if we're using the demo workspace for testing
            if self._workspace_id == "DEMO_WORKSPACE":
                self._authentication_policy = AzureKeyCredentialPolicy(
                    name="X-Api-Key", 
                    credential=self.credential
                )
            else:
                self._authentication_policy = None
        return self._authentication_policy

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        """Get an access token for the specified scopes.
        
        Args:
            *scopes: The scopes to request access for.
            **kwargs: Additional arguments passed to get_token.
            
        Returns:
            Access token for the specified scopes.
        """
        # AzureKeyCredential doesn't have get_token method, so we need to handle it differently
        if isinstance(self.credential, AzureKeyCredential):
            # For AzureKeyCredential, we don't need to get tokens - it's used directly
            # This method shouldn't be called for AzureKeyCredential, but if it is,
            # we'll raise a helpful error
            raise NotImplementedError(
                "AzureKeyCredential doesn't support token-based authentication. "
                "It should be used directly with the LogsQueryClient."
            )
        else:
            # For DefaultAzureCredential, use the normal get_token method
            return self.credential.get_token(*scopes, **kwargs)
