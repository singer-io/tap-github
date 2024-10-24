from functools import cached_property
from typing import Any, Mapping, TypedDict, TypeGuard
import os
import sys
import time
import requests



class OAuthCredentials(TypedDict):
    client_id: str
    client_secret: str
    installation_id: str


def is_oauth_credentials(value: Mapping[str, Any]) -> TypeGuard[OAuthCredentials]:
    return "installation_id" in value
