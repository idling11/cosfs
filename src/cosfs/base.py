"""
Code of base class of OSSFileSystem
"""
import logging
import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from fsspec.spec import AbstractFileSystem
from fsspec.utils import stringify_path
from qcloud_cos import CosS3Client

from .file import OSSFile

logger = logging.getLogger("cosfs")
logging.getLogger("oss2").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.WARNING)

if TYPE_CHECKING:
    from oss2.models import SimplifiedObjectInfo


DEFAULT_POOL_SIZE = 20
DEFAULT_BLOCK_SIZE = 5 * 2**20
SIMPLE_TRANSFER_THRESHOLD = 100 * 1024 * 1024


class BaseCOSFileSystem(AbstractFileSystem):
    # pylint: disable=abstract-method

    """
    base class of the cosfs (cosfs) OSS file system access OSS(Object
    Storage Service) as if it were a file system.

    This exposes a filesystem-like API (ls, cp, open, etc.) on top of OSS
    storage.

    Provide credentials with `key` and `secret`, or together with `token`.
    If anonymous leave all these argument empty.

    """

    protocol = "cos"

    def __init__(
        self,
        endpoint: Optional[str] = None,
        key: Optional[str] = None,
        secret: Optional[str] = None,
        token: Optional[str] = None,
        default_cache_type: str = "readahead",
        default_block_size: Optional[int] = None,
        **kwargs,  # pylint: disable=too-many-arguments
    ):
        """
        Parameters
        ----------
        endpoint: string (None)
            Default endpoints of the fs Endpoints are the adderss where OSS
            locate like: http://oss-cn-hangzhou.aliyuncs.com or
            https://oss-me-east-1.aliyuncs.com, Can be changed after the
            initialization.
        key : string (None)
            If not anonymous, use this access key ID, if specified.
        secret : string (None)
            If not anonymous, use this secret access key, if specified.
        token : string (None)
            If not anonymous, use this security token, if specified.
        default_block_size: int (None)
            If given, the default block size value used for ``open()``, if no
            specific value is given at all time. The built-in default is 5MB.
        default_cache_type : string ("readahead")
            If given, the default cache_type value used for ``open()``. Set to "none"
            if no caching is desired. See fsspec's documentation for other available
            cache_type values. Default cache_type is "readahead".

        The following parameters are passed on to fsspec:

        skip_instance_cache: to control reuse of instances
        use_listings_cache, listings_expiry_time, max_paths: to control reuse of
        directory listings
        """

        self._endpoint = endpoint or os.getenv("COS_ENDPOINT")
        if self._endpoint is None:
            logger.warning(
                "COS endpoint is not set, COSFS could not work properly"
                "without a endpoint"
            )

        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # passed to fsspec superclass
        super().__init__(**super_kwargs)

        self._default_block_size = default_block_size or DEFAULT_BLOCK_SIZE
        self._default_cache_type = default_cache_type

    def set_endpoint(self, endpoint: str):
        """
        Reset the endpoint for cosfs
        endpoint : string (None)
            Default endpoints of the fs
            Endpoints are the adderss where OSS locate
            like: http://oss-cn-hangzhou.aliyuncs.com or
        """
        if not endpoint:
            raise ValueError("Not a valid endpoint")
        self._endpoint = endpoint

    @classmethod
    def _strip_protocol(cls, path):
        """Turn path from fully-qualified to file-system-specifi
        Parameters
        ----------
        path : Union[str, List[str]]
            Input path, like
            `cos://mybucket/myobject`
        Examples
        --------
        >>> _strip_protocol(
            "oss://mybucket/myobject"
            )
        ('/mybucket/myobject')
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]
        path_string: str = stringify_path(path)
        if path_string.startswith("cos://"):
            path_string = path_string[5:]
        return path_string or cls.root_marker

    def split_path(self, path: str) -> Tuple[str, str]:
        """
        Normalise object path string into bucket and key.
        Parameters
        ----------
        path : string
            Input path, like `/mybucket/path/to/file`
        Examples
        --------
        >>> split_path("/mybucket/path/to/file")
        ['mybucket', 'path/to/file' ]
        >>> split_path("
        /mybucket/path/to/versioned_file?versionId=some_version_id
        ")
        ['mybucket', 'path/to/versioned_file', 'some_version_id']
        """
        path = self._strip_protocol(path)
        path = path.lstrip("/")
        if "/" not in path:
            return path, ""
        bucket_name, obj_name = path.split("/", 1)
        return bucket_name, obj_name

    def invalidate_cache(self, path: Optional[str] = None):
        if path is None:
            self.dircache.clear()
        else:
            norm_path: str = self._strip_protocol(path)
            norm_path = norm_path.lstrip("/")
            self.dircache.pop(norm_path, None)
            while norm_path:
                self.dircache.pop(norm_path, None)
                norm_path = self._parent(norm_path)

    def _verify_find_arguments(
        self, path: str, maxdepth: Optional[int], withdirs: bool, prefix: str
    ) -> str:
        path = self._strip_protocol(path)
        bucket, _ = self.split_path(path)
        if not bucket:
            raise ValueError("Cannot traverse all of the buckets")
        if (withdirs or maxdepth) and prefix:
            raise ValueError(
                "Can not specify 'prefix' option alongside "
                "'withdirs'/'maxdepth' options."
            )
        return path

    def _get_batch_delete_key_list(self, pathlist: List[str]) -> Tuple[str, List[str]]:
        buckets: Set[str] = set()
        key_list: List[str] = []
        for path in pathlist:
            bucket, key = self.split_path(path)
            buckets.add(bucket)
            key_list.append(key)
        if len(buckets) > 1:
            raise ValueError("Bulk delete files should refer to only one bucket")
        bucket = buckets.pop()
        if len(pathlist) > 1000:
            raise ValueError("Max number of files to delete in one call is 1000")
        for path in pathlist:
            self.invalidate_cache(self._parent(path))
        return bucket, key_list

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        autocommit: bool = True,
        cache_options: Optional[str] = None,
        **kwargs,  # pylint: disable=too-many-arguments
    ) -> "OSSFile":
        """
        Open a file for reading or writing.
        Parameters
        ----------
        path: str
            File location
        mode: str
            'rb', 'wb', etc.
        autocommit: bool
            If False, writes to temporary file that only gets put in final
            location upon commit
        kwargs
        Returns
        -------
        OSSFile instance
        """
        cache_type = kwargs.pop("cache_type", self._default_cache_type)
        return OSSFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            cache_type=cache_type,
            **kwargs,
        )

    def touch(self, path: str, truncate: bool = True, **kwargs):
        """Create empty file, or update timestamp

        Parameters
        ----------
        path: str
            file location
        truncate: bool
            If True, always set file size to 0; if False, update timestamp and
            leave file unchanged, if backend allows this
        """
        if truncate or not self.exists(path):
            self.invalidate_cache(self._parent(path))
            with self.open(path, "wb", **kwargs):
                pass
        else:
            raise NotImplementedError
