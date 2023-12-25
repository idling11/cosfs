"""
COSFS
----------------------------------------------------------------
A pythonic file-systems interface to COS (Object Storage Service)
"""

from .core import OSSFileSystem
from .file import OSSFile

__all__ = ["OSSFile", "OSSFileSystem"]
