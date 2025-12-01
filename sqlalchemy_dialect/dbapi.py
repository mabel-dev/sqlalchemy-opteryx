"""DBAPI 2.0 (PEP 249) compliant interface for Opteryx data service.

This module implements a minimal DBAPI 2.0 interface that communicates
with the Opteryx data service via HTTP. It provides Connection and Cursor
classes that translate SQL queries into HTTP requests.
"""

from __future__ import annotations

import json
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from urllib.parse import urljoin

import requests

# Module globals required by PEP 249
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "named"  # Named style: WHERE name=:name


class Error(Exception):
    """Base exception for DBAPI errors."""


class Warning(Exception):  # noqa: A001
    """Warning exception."""


class InterfaceError(Error):
    """Exception for interface errors."""


class DatabaseError(Error):
    """Exception for database errors."""


class DataError(DatabaseError):
    """Exception for data errors."""


class OperationalError(DatabaseError):
    """Exception for operational errors."""


class IntegrityError(DatabaseError):
    """Exception for integrity constraint errors."""


class InternalError(DatabaseError):
    """Exception for internal errors."""


class ProgrammingError(DatabaseError):
    """Exception for programming errors."""


class NotSupportedError(DatabaseError):
    """Exception for not supported operations."""


# Type constructors (required by PEP 249)
def Date(year: int, month: int, day: int) -> str:
    """Construct a date value."""
    return f"{year:04d}-{month:02d}-{day:02d}"


def Time(hour: int, minute: int, second: int) -> str:
    """Construct a time value."""
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def Timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> str:
    """Construct a timestamp value."""
    return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"


def Binary(string: bytes) -> bytes:
    """Construct a binary value."""
    return string


STRING = str
BINARY = bytes
NUMBER = float
DATETIME = str
ROWID = str

class Cursor:
    """DBAPI 2.0 Cursor implementation for Opteryx."""
    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._jwt_token: Optional[str] = None
        self._description: Optional[
            List[Tuple[str, Any, None, None, None, None, Optional[bool]]]
        ] = None
        self._rowcount = -1
        self._rows: List[Tuple[Any, ...]] = []
        self._row_index = 0
        self._arraysize = 1
        self._closed = False
        self._statement_handle: Optional[str] = None

        # Try to authenticate using client credentials (client credentials flow)
        # client_id is connection._username and client_secret is connection._token
        try:
            username = getattr(self._connection, "_username", None)
            secret = getattr(self._connection, "_token", None)
            if username and secret:
                host = getattr(self._connection, "_host", "localhost")
                # Normalize domain and build auth host (auth.domain)
                try:
                    domain = self._connection._normalize_domain(host)
                except Exception:
                    domain = host
                # Only add auth. prefix when domain looks like a DNS name (not 'localhost')
                if "." in domain and not domain.startswith("localhost"):
                    auth_host = f"auth.{domain}"
                else:
                    auth_host = domain
                scheme = "https" if getattr(self._connection, "_ssl", False) else "http"
                auth_url = f"{scheme}://{auth_host}/token"

                # Build form-encoded payload
                payload = {
                    "grant_type": "client_credentials",
                    "client_id": username,
                    "client_secret": secret,
                }
                # Use the connection session for auth so auth header set for all subsequent calls
                sess = getattr(self._connection, "_session", requests.Session())
                headers = {"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
                resp = sess.post(auth_url, data=payload, headers=headers, timeout=getattr(self._connection, "_timeout", 30))
                resp.raise_for_status()
                body = resp.json() if resp.text else {}
                token = body.get("access_token") or body.get("token") or body.get("jwt")
                if token:
                    self._jwt_token = token
                    # Set Authorization header for subsequent requests via the connection session
                    try:
                        self._connection._session.headers["Authorization"] = f"Bearer {token}"
                    except Exception:
                        # If connection session is not available or some other issue, ignore gracefully
                        pass
        except requests.exceptions.RequestException:
            # Authentication failed — don't raise here; we will attempt queries without the JWT
            self._jwt_token = None
        except Exception:
            # Any unexpected failure in auth should not crash cursor creation
            self._jwt_token = None

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, Any, None, None, None, None, Optional[bool]]]]:
        """Column description as required by PEP 249."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last operation."""
        return self._rowcount

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    def close(self) -> None:
        """Close the cursor."""
        self._closed = True
        self._rows = []
        self._description = None

    def _check_closed(self) -> None:
        """Raise exception if cursor is closed."""
        if self._closed:
            raise ProgrammingError("Cursor is closed")

    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Dict[str, Any], Sequence[Any]]] = None,
    ) -> "Cursor":
        """Execute a SQL statement.

        Args:
            operation: SQL statement to execute
            parameters: Optional parameters for the statement

        Returns:
            Self for method chaining
        """
        self._check_closed()
        self._rows = []
        self._row_index = 0
        self._description = None
        self._rowcount = -1

        # Convert sequence parameters to dict if needed
        params_dict: Optional[Dict[str, Any]] = None
        if parameters is not None:
            if isinstance(parameters, dict):
                params_dict = parameters
            else:
                # Convert positional to named parameters
                params_dict = {f"p{i}": v for i, v in enumerate(parameters)}
                # Replace ? placeholders with :p0, :p1, etc.
                for i in range(len(parameters)):
                    operation = operation.replace("?", f":p{i}", 1)

        # Submit the statement
        response = self._connection._submit_statement(operation, params_dict)
        self._statement_handle = response.get("statementHandle")

        if not self._statement_handle:
            raise DatabaseError("No statement handle returned from server")

        # Poll for completion
        self._poll_for_results()

        return self

    def _poll_for_results(self) -> None:
        """Poll the server until statement execution completes."""
        if not self._statement_handle:
            return

        max_wait = 300  # Maximum wait time in seconds
        poll_interval = 0.5
        elapsed = 0.0

        while elapsed < max_wait:
            status = self._connection._get_statement_status(self._statement_handle)
            state = status.get("status", {}).get("state", "UNKNOWN")

            if state in ("SUCCEEDED", "SUCCESS", "COMPLETED"):
                # Fetch results
                self._fetch_results()
                return
            elif state in ("FAILED", "ERROR", "CANCELLED"):
                description = status.get("status", {}).get("description", "Unknown error")
                raise DatabaseError(f"Statement execution failed: {description}")
            elif state in ("SUBMITTED", "RUNNING", "PENDING", "EXECUTING"):
                time.sleep(poll_interval)
                elapsed += poll_interval
                # Exponential backoff up to 5 seconds
                poll_interval = min(poll_interval * 1.5, 5.0)
            else:
                raise DatabaseError(f"Unknown statement state: {state}")

        raise OperationalError("Statement execution timed out")

    def _fetch_results(self) -> None:
        """Fetch results from a completed statement."""
        if not self._statement_handle:
            return

        page_size = max(1, self._arraysize)
        offset = 0
        has_description = False
        rows: List[Tuple[Any, ...]] = []
        total_rows: Optional[int] = None

        while True:
            result = self._connection._get_statement_results(
                self._statement_handle, num_rows=page_size, offset=offset
            )

            # Set total_rows if provided
            if total_rows is None and "total_rows" in result:
                try:
                    total_rows = int(result.get("total_rows", 0))
                except Exception:
                    total_rows = None

            # Set description if present
            columns_meta = result.get("columns", [])
            if columns_meta and not has_description:
                self._description = [
                    (col.get("name", f"col{i}"), None, None, None, None, None, None)
                    for i, col in enumerate(columns_meta)
                ]
                has_description = True

            data = result.get("data", [])
            if data:
                # Columnar form: List[Dict[name,type,values]]
                if isinstance(data[0], dict) and "values" in data[0]:
                    if not has_description:
                        self._description = [
                            (col.get("name", f"col{i}"), None, None, None, None, None, None)
                            for i, col in enumerate(data)
                        ]
                        has_description = True
                    values_lists = [col.get("values", []) for col in data]
                    if values_lists:
                        n_local = len(values_lists[0])
                        for i in range(n_local):
                            row = tuple(values_lists[j][i] for j in range(len(values_lists)))
                            rows.append(row)
                else:
                    # Row-oriented data (list of rows)
                    for row in data:
                        rows.append(tuple(row))

            fetched_this_page = len(rows) - offset
            if fetched_this_page <= 0:
                # Nothing more to fetch; break to avoid infinite loop
                break

            offset += fetched_this_page

            if total_rows is not None and offset >= total_rows:
                break

            # Continue if next_page is present; many APIs expose next_page; if not, stop when we see no rows
            if not result.get("next_page") and (total_rows is None or offset >= total_rows):
                break

        # Finalize rows and counts
        self._rows = rows
        self._rowcount = len(self._rows)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Union[Dict[str, Any], Sequence[Any]]],
    ) -> "Cursor":
        """Execute a SQL statement multiple times with different parameters."""
        self._check_closed()
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        return self

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetch the next row of a query result set."""
        self._check_closed()
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """Fetch the next set of rows."""
        self._check_closed()
        if size is None:
            size = self._arraysize
        rows = self._rows[self._row_index : self._row_index + size]
        self._row_index += len(rows)
        return rows

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """Fetch all remaining rows."""
        self._check_closed()
        rows = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return rows

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """Set input sizes (no-op, but required by PEP 249)."""
        pass

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """Set output size (no-op, but required by PEP 249)."""
        pass

    def __iter__(self) -> "Cursor":
        """Make cursor iterable."""
        return self

    def __next__(self) -> Tuple[Any, ...]:
        """Get next row."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Connection:
    """DBAPI 2.0 Connection implementation for Opteryx.

    Manages HTTP connections to the Opteryx data service.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8000,
        username: Optional[str] = None,
        token: Optional[str] = None,
        database: Optional[str] = None,
        ssl: bool = False,
        timeout: float = 30.0,
    ) -> None:
        """Initialize connection to Opteryx data service.

        Args:
            host: Hostname of the Opteryx data service
            port: Port number
            username: Username for authentication (optional)
            token: Bearer token for authentication
            database: Database/schema name (optional)
            ssl: Whether to use HTTPS
            timeout: Request timeout in seconds
        """
        self._host = host
        self._port = port
        self._username = username
        self._token = token
        self._database = database
        self._ssl = ssl
        self._timeout = timeout
        self._closed = False

        # Build base URL
        scheme = "https" if ssl else "http"
        if (ssl and port == 443) or (not ssl and port == 80):
            self._base_url = f"{scheme}://{host}"
        else:
            self._base_url = f"{scheme}://{host}:{port}"

        # Create session for connection pooling
        self._session = requests.Session()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.headers["Content-Type"] = "application/json"

    def _normalize_domain(self, host: str) -> str:
        """Return the base domain for the given host by stripping known subdomain prefixes.

        Examples:
            'data.opteryx.app' -> 'opteryx.app'
            'auth.opteryx.app' -> 'opteryx.app'
            'opteryx.app' -> 'opteryx.app'
            'localhost' -> 'localhost'
        """
        domain = host
        for p in ("data.", "auth."):
            if domain.startswith(p):
                domain = domain[len(p) :]
        return domain

    def _data_base_url(self) -> str:
        """Construct a base URL that targets the 'data' subdomain for API requests."""
        scheme = "https" if self._ssl else "http"
        domain = self._normalize_domain(self._host)
        # Only add subdomain prefix for DNS-style hosts (e.g. example.com), not for localhost or IPs
        if "." in domain and not domain.startswith("localhost"):
            data_host = f"data.{domain}"
        else:
            data_host = domain
        if (self._ssl and self._port == 443) or (not self._ssl and self._port == 80):
            return f"{scheme}://{data_host}"
        return f"{scheme}://{data_host}:{self._port}"

    def _check_closed(self) -> None:
        """Raise exception if connection is closed."""
        if self._closed:
            raise ProgrammingError("Connection is closed")

    def _submit_statement(
        self, sql: str, parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit a SQL statement to the data service."""
        self._check_closed()

        url = urljoin(self._data_base_url() + "/", "api/v1/statements")
        payload: Dict[str, Any] = {"sqlText": sql}
        if parameters:
            payload["parameters"] = parameters

        try:
            response = self._session.post(url, json=payload, timeout=self._timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                try:
                    detail = e.response.json().get("detail", str(e))
                except (ValueError, json.JSONDecodeError):
                    detail = e.response.text or str(e)
                raise DatabaseError(f"HTTP error: {detail}") from e
            raise DatabaseError(f"HTTP error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise OperationalError(f"Connection error: {e}") from e

    def _get_statement_status(self, statement_handle: str) -> Dict[str, Any]:
        """Get the status of a submitted statement."""
        self._check_closed()

        url = urljoin(self._data_base_url() + "/", f"api/v1/statements/{statement_handle}")

        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise ProgrammingError("Statement not found") from e
            raise DatabaseError(f"HTTP error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise OperationalError(f"Connection error: {e}") from e

    def _get_statement_results(
        self, statement_handle: str, num_rows: Optional[int] = None, offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get results for a completed statement.

        Note: The current API may return results as part of status response
        or via a separate results endpoint. This method handles both cases.
        """
        # Try to get results from the status endpoint first
        # The API design may evolve to have a separate results endpoint
        status = self._get_statement_status(statement_handle)

        # If results are embedded in status response
        if "data" in status or "columns" in status:
            return status

        # Try a dedicated results endpoint if it exists
        url = urljoin(
            self._data_base_url() + "/", f"api/v1/statements/{statement_handle}/results"
        )
        params: Dict[str, Any] = {}
        if num_rows is not None:
            params["num_rows"] = int(num_rows)
        if offset is not None:
            params["offset"] = int(offset)
        try:
            response = self._session.get(url, params=params or None, timeout=self._timeout)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException:
            pass

        # Return status response (may contain partial result info)
        return status

    def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self._session.close()
            self._closed = True

    def commit(self) -> None:
        """Commit transaction (no-op for Opteryx as it's read-only)."""
        self._check_closed()

    def rollback(self) -> None:
        """Rollback transaction (no-op for Opteryx as it's read-only)."""
        self._check_closed()

    def cursor(self) -> Cursor:
        """Create a new cursor object."""
        self._check_closed()
        return Cursor(self)

    def __enter__(self) -> "Connection":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


def connect(
    host: str = "localhost",
    port: int = 8000,
    username: Optional[str] = None,
    token: Optional[str] = None,
    database: Optional[str] = None,
    ssl: bool = False,
    timeout: float = 30.0,
) -> Connection:
    """Create a new connection to the Opteryx data service.

    Args:
        host: Hostname of the Opteryx data service
        port: Port number
        username: Username for authentication (optional)
        token: Bearer token for authentication
        database: Database/schema name (optional)
        ssl: Whether to use HTTPS
        timeout: Request timeout in seconds

    Returns:
        A new Connection object
    """
    return Connection(
        host=host,
        port=port,
        username=username,
        token=token,
        database=database,
        ssl=ssl,
        timeout=timeout,
    )