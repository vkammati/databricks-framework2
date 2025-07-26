"""
Create AAD Token using SPN
"""
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
import msal
import requests
from requests.exceptions import HTTPError


class AzureSPNWithCertificate:
    """SPN with certificate.
    
    Example:
    >>> pfx_path = pass
    >>> tenant_id = pass
    >>> spn_app_id = pass
    >>> certificate_thumbprint = pass
    >>> pfx_password = pass
    >>> spn = AzureSPNWithCertificate(tenant_id, spn_app_id, certificate_thumbprint, pfx_password)
    >>> spn.read_pfx_file(pfx_path)
    >>> ad_token = spn.generate_ad_token()
    """
    def __init__(
            self,
            tenant_id: str,
            spn_app_id: str,
            certificate_thumbprint: str,
            pfx_password: str = None,
            pfx_bytes: bytes = None):
        """Instantiate an AzureSPNWithCertificate class.
        
        :param tenant_id: Azure tenant id
        :param spn_app_id: SPN application (client) id
        :param certificate_thumbprint: SPN's certificate thumbprint
        :param pfx_password: password used for the certificate
        :param pfx_bytes: certificate file as bytes. If None, you must call function read_pfx_file to read it.
        """
        self.tenant_id = tenant_id
        self.spn_app_id = spn_app_id
        self.pfx_bytes = pfx_bytes
        self.pfx_password = pfx_password
        self.certificate_thumbprint = certificate_thumbprint
        self.private_key_bytes = None
    
    def read_pfx_file(self, pfx_path: str):
        """Read certificate and store it as bytes parameter 'pfx_bytes'.
        
        :param pfx_path: path to the certificate file (pfx file)
        """
        with open(pfx_path, "rb") as f_pfx:
            self.pfx_bytes = f_pfx.read()
            
    # see https://stackoverflow.com/questions/6345786/python-reading-a-pkcs12-certificate-with-pyopenssl-crypto
    def _set_private_key_from_certificate(self) -> bytes:
        """Retrieve the private key from the certificate and store it as bytes parameter 'private_key_bytes'."""
        if not self.pfx_bytes:
            raise ValueError(f"Parameter 'pfx_bytes' is missing.")
            
        private_key, _, _ = load_key_and_certificates(self.pfx_bytes, self.pfx_password.encode())
        
        self.private_key_bytes = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
    
    def generate_ad_token(self) -> str:
        """Generate a short lived AD token for the SPN."""
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        azure_dbx_scope = ["2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"]
        
        if not self.private_key_bytes:
            self._set_private_key_from_certificate()
            
        app = msal.ConfidentialClientApplication(
            self.spn_app_id,
            authority=authority,
            client_credential={
                "thumbprint": self.certificate_thumbprint,
                "private_key": self.private_key_bytes,
            }
        )

        result = app.acquire_token_for_client(scopes=azure_dbx_scope)
        return result["access_token"]
    

class AzureSPN:
    """SPN with secret

    Example:
    >>> tenant_id = pass
    >>> spn_client_id = pass
    >>> spn_client_secret = pass
    >>> spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
    >>> ad_token = spn.generate_ad_token()
    """
    def __init__(self, tenant_id, spn_client_id, spn_client_secret):
        self.tenant_id = tenant_id
        self.spn_client_id = spn_client_id
        self.spn_client_secret = spn_client_secret
    
    def generate_ad_token(self) -> str:
        """Generate a short lived AD token for the SPN."""
        req = requests.post(
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data={
                "client_id": self.spn_client_id,
                "grant_type": "client_credentials",
                "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
                "client_secret": self.spn_client_secret
            }
        )

        try:
            req.raise_for_status()
        except HTTPError as e:
            print(e.response.text)
            raise e

        return req.json()["access_token"]
    