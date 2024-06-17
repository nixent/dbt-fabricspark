from __future__ import annotations
import json
import time
import requests
from requests.models import Response
from urllib import response
import re
import datetime as dt
from types import TracebackType
from typing import Any

from dbt_common.exceptions import DbtDatabaseError
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.utils.encoding import DECIMALS
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt.adapters.fabricspark.fabric_spark_credentials import SparkCredentials
from dbt.adapters.fabricspark.shortcuts import ShortcutClient
from dbt.adapters.fabricspark.livyenums import LivyStates

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

livysession_credentials: SparkCredentials

DEFAULT_SESSION_POLL_WAIT = 10
DEFAULT_STATEMENT_POLL_WAIT = 2
AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
accessToken: AccessToken = None


def is_token_refresh_necessary(unixTimestamp: int) -> bool:
    # Convert to datetime object
    dt_object = dt.datetime.fromtimestamp(unixTimestamp)
    # Convert to local time
    local_time = time.localtime(time.time())

    # Calculate difference
    difference = dt_object - dt.datetime.fromtimestamp(time.mktime(local_time))
    if int(difference.total_seconds() / 60) < 5:
        logger.debug(f"Token Refresh necessary in {int(difference.total_seconds() / 60)}")
        return True
    else:
        return False


def get_cli_access_token(credentials: SparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    accessToken = AzureCliCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    logger.debug("CLI - Fetched Access Token")
    return accessToken


def get_sp_access_token(credentials: SparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    accessToken = ClientSecretCredential(
        str(credentials.tenant_id), str(credentials.client_id), str(credentials.client_secret)
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    logger.info("SPN - Fetched Access Token")
    return accessToken


def get_headers(credentials: SparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    global accessToken
    if accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
        if credentials.authentication and credentials.authentication.lower() == "cli":
            logger.debug("Using CLI auth")
            accessToken = get_cli_access_token(credentials)
        else:
            logger.debug("Using SPN auth")
            accessToken = get_sp_access_token(credentials)

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken.token}"}
    if tokenPrint:
        logger.debug(accessToken.token)

    return headers


class LivySession:
    def __init__(self, credentials: SparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.livy_endpoint
        self.session_id = None
        self.is_new_session_required = True

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        # self.delete_session()
        return True
    
    def _await_session_start(self):

        start_time = time.time()
        elapsed_time = 0
 
        while True:
            elapsed_time = time.time() - start_time
            remaining_time = self.credentials.session_provisioning_timeout - elapsed_time
            
            res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            ).json()

            if res["state"] in [LivyStates.NOT_STARTED, LivyStates.STARTING]:                
                # logger.debug("Polling Session creation status - ", self.connect_url + '/sessions/' + self.session_id )
                time.sleep(min(DEFAULT_SESSION_POLL_WAIT, remaining_time))
            elif res["livyInfo"]["currentState"] == LivyStates.IDLE:
                logger.debug(f"Started livy session {self.session_id} in {elapsed_time} seconds")
                self.is_new_session_required = False
                break
            elif res["livyInfo"]["currentState"] == LivyStates.DEAD:
                print("ERROR, cannot create a livy session")
                raise FailedToConnectError("Failed to create a livy session")
            
            if remaining_time <= 0:
                self.delete_session()
                raise TimeoutError(f"Livy session took more than {self.credentials.session_provisioning_timeout} seconds to start")

    def create_session(self, data) -> str:
        response = None
        logger.debug(f"Creating livy session...")
        try:
            response = requests.post(
                self.connect_url + "/sessions",
                data=json.dumps(data),
                headers=get_headers(self.credential, True),
            )
            if response.status_code == 200:
                logger.debug("Initiated Livy Session...")
            response.raise_for_status()
        except requests.exceptions.ConnectionError as c_err:
            print("Connection Error :", c_err)
        except requests.exceptions.HTTPError as h_err:
            print("Http Error: ", h_err)
        except requests.exceptions.Timeout as t_err:
            print("Timeout Error: ", t_err)
        except requests.exceptions.RequestException as a_err:
            print("Authorization Error: ", a_err)

        if response is None:
            raise Exception("Invalid response from livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()["id"])
        except requests.exceptions.JSONDecodeError as json_err:
            raise Exception("Json decode error to get session_id") from json_err

        self._await_session_start()
        
        return self.session_id

    def delete_session(self) -> None:
        logger.debug(f"Closing the livy session: {self.session_id}")

        try:
            # delete the livy session
            _ = requests.delete(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            )
            if _.status_code == 200:
                logger.debug(f"Closed the livy session: {self.session_id}")
            else:
                response.raise_for_status()

        except Exception as ex:
            logger.error(f"Unable to close the livy session {self.session_id}, error: {ex}")

    def is_valid_session(self) -> bool:
        res = requests.get(
            self.connect_url + "/sessions/" + self.session_id,
            headers=get_headers(self.credential, False),
        ).json()

        valid_states = [LivyStates.IDLE, LivyStates.RUNNING, LivyStates.SUCCESS, LivyStates.BUSY]

        return res["livyInfo"]["currentState"] in valid_states


# cursor object - wrapped for livy API
class LivyCursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, credential, livy_session) -> None:
        self._rows = None
        self._schema = None
        self.credential = credential
        self.connect_url = credential.livy_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session

    def __enter__(self) -> LivyCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        """
        Get the description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field["name"],
                    field["type"],  # field['dataType'],
                    None,
                    None,
                    None,
                    None,
                    field["nullable"],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._rows = None

    def _submitLivyCode(self, code) -> Response:
        if self.livy_session.is_new_session_required:
            LivySessionManager.connect(self.credential)
            self.session_id = self.livy_session.session_id

        # Submit code
        data = {"code": code, "kind": "sql"}
        logger.debug(
            f"Submitted: {data} {self.connect_url + '/sessions/' + self.session_id + '/statements'}"
        )

        res = requests.post(
            self.connect_url + "/sessions/" + self.session_id + "/statements",
            data=json.dumps(data),
            headers=get_headers(self.credential, False),
        )

        return res

    def _getLivySQL(self, sql) -> str:
        """ 
        Format SQL statement. Add trailing space if necessary.
        If query finishes with single quote ('),
        the execution of the query will fail. Ex: WHERE column='foo'
        """
        # Remove comments
        formatted_sql = re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, re.DOTALL).strip()
        
        if formatted_sql.endswith("'"):
            return formatted_sql + " "
        
        return formatted_sql

    def _cancel_statement(self, statement_id) -> None:
        logger.debug(f"Cancelling statement: {statement_id}")
        try:
            _ = requests.post(
                self.connect_url
                + "/sessions/"
                + self.session_id
                + "/statements/"
                + statement_id
                + "/cancel",
                headers=get_headers(self.credential, False),
            )
            if _.status_code == 200:
                logger.debug(f"Cancelled statement: {statement_id}")
            else:
                response.raise_for_status()
        except Exception as ex:
            logger.error(f"Unable to cancel the statement {statement_id}, error: {ex}")

    def _getLivyResult(self, res_obj) -> Response:
        start_time = time.time()
        elapsed_time = 0
        res = None
 
        json_res = res_obj.json()
        statement_id = repr(json_res["id"])
 
        while True:
            elapsed_time = time.time() - start_time
            remaining_time = self.credentials.session_provisioning_timeout - elapsed_time

            res = requests.get(
                self.connect_url
                + "/sessions/"
                + self.session_id
                + "/statements/"
                + statement_id,
                headers=get_headers(self.credential, False),
            ).json()

            if res.state in [LivyStatementStates.AVAILABLE, LivyStatementStates.ERROR, LivyStatementStates.CANCELLING, LivyStatementStates.CANCELLED]:
                break
            time.sleep(DEFAULT_STATEMENT_POLL_WAIT)

            if remaining_time <= 0:
                self._cancel_statement(statement_id)
                raise TimeoutError(f"Query took more than {self.credentials.query_timeout} seconds to run")
            
            return res

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters

        # TODO: handle parameterised sql

        res = self._getLivyResult(self._submitLivyCode(self._getLivySQL(sql)))
        logger.debug(f"Livy statement result: {res}")
        if res["output"]["status"] == "ok":
            # values = res['output']['data']['application/json']
            values = res["output"]["data"]["application/json"]
            if len(values) >= 1:
                self._rows = values["data"]  # values[0]['values']
                self._schema = values["schema"]["fields"]  # values[0]['schema']
                # print("rows", self._rows)
                # print("schema", self._schema)
            else:
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise DbtDatabaseError(
                "Error while executing query: " + res["output"]["evalue"]
            )

    def fetchall(self):
        """
        Fetch all data.

        Returns
        -------
        out : list() | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self):
        """
        Fetch the first output.

        Returns
        -------
        out : one row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """

        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


class LivyConnection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(self, credentials, livy_session) -> None:
        self.credentials: SparkCredentials = credentials
        self.connect_url = credentials.livy_endpoint
        self.session_id = livy_session.session_id
        self.livy_session_parameters = credentials.livy_session_parameters

        self._cursor = LivyCursor(self.credentials, livy_session)

    def get_session_id(self) -> str:
        return self.session_id

    def get_headers(self) -> dict[str, str]:
        return get_headers(self.credentials, False)

    def get_connect_url(self) -> str:
        return self.connect_url

    def cursor(self) -> LivyCursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True


# TODO: How to authenticate
class LivySessionManager:
    livy_global_session = None

    @staticmethod
    def connect(credentials: SparkCredentials) -> LivyConnection:
        # the following opens an spark / sql session
        data = {"kind": "sql", "conf": credentials.livy_session_parameters}  # 'spark'
        if __class__.livy_global_session is None:
            __class__.livy_global_session = LivySession(credentials)
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
            # create shortcuts, if there are any
            if credentials.shortcuts_json_path:
                shortcut_client = ShortcutClient(accessToken.token, credentials.workspaceid, credentials.lakehouseid)
                shortcut_client.create_shortcuts(credentials.shortcuts_json_path)
        elif not __class__.livy_global_session.is_valid_session():
            __class__.livy_global_session.delete_session()
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
        elif __class__.livy_global_session.is_new_session_required:
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
        else:
            logger.debug(f"Reusing session: {__class__.livy_global_session.session_id}")
        livyConnection = LivyConnection(credentials, __class__.livy_global_session)
        return livyConnection

    @staticmethod
    def disconnect() -> None:
        if __class__.livy_global_session.is_valid_session():
            __class__.livy_global_session.delete_session()
            __class__.livy_global_session.is_new_session_required = True


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy session connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self) -> LivySessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

    def close(self) -> None:
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            return f"'{value}'"
