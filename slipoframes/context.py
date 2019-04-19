"""
SLIPO API client wrapper.

This class provides methods for accessing `SLIPO API` functionality
from Jupyter notebooks
"""

import math
import pandas

from typing import Union, Tuple
from datetime import datetime

from slipo.client import Client

InputType = Union[str, Tuple[int, int]]

# Default API endpoint
BASE_URL = 'https://app.dev.slipo.eu/'


class SlipoContext(Client):
    """A class that wraps :py:class:`Client <slipo.client.Client>` object.

    Args:
        api_key (str): SLIPO API key.
        base_url (str, optional): Base URL for SLIPO API endpoints. The default
            value is ``https://app.dev.slipo.eu/``.
        requires_ssl (bool, optional): If `False`, unsecured connections are allowed (default `True`).

    Returns:
        A :py:class:`SlipoContext <slipoframes.context.SlipoContext>` object.

    """

    def __init__(self, api_key: str, base_url: str = None, requires_ssl: bool = True):
        super().__init__(api_key=api_key, base_url=base_url, requires_ssl=requires_ssl)

    def _to_datetime(self, value: str or None) -> datetime or None:
        if value is None:
            return None
        if math.isnan(value):
            return None

        return datetime.fromtimestamp(value / 1000)

    def _format_file_size(self, num: int, suffix: str = 'B') -> str:
        for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E']:
            if abs(num) < 1024.0:
                return '%3.1f %s%s' % (num, unit, suffix)
            num /= 1024.0

        return '%.1f%s%s' % (num, 'Z', suffix)

    def _flatten_file_system(self, obj: dict, result: list) -> list:
        if not type(obj) is dict:
            return

        if 'files' in obj:
            for f in obj['files']:
                result.append({
                    'name': f['name'],
                    'modified': self._to_datetime(f['modified']),
                    'size': f['size'],
                    'path': f['path']
                })

        if 'folders' in obj and type(obj['folders']) is list:
            for folder in obj['folders']:
                self._flatten_file_system(folder, result)

        return result

    def file_browse(self, sort_col: str = 'modified', sort_asc: bool = True, format_size: bool = False) -> pandas.DataFrame:
        """Browse all files and folders on the remote file system.

        Args:
            sort_col (str): Sorting column name (default `modified`).
            sort_asc (bool, optional): Sets ascending sort order (default `True`).
            format_size (bool, optional): If `True`, the file size is converted
                to a user friendly string (default `False`).

        Returns:
            A :obj:`pandas.DataFrame` with all files 

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        # File system as JSON
        result = super().file_browse()

        # Flatten file system hierarchy
        data = self._flatten_file_system(result, [])

        # Create data frame
        df = pandas.DataFrame(data=data)

        # Check if sort column exists
        if not sort_col in df:
            print('Column {sort_col} was not found.'
                  ' Sorting by column modified'.format(sort_col=sort_col))
            sort_col = 'modified'

        df = df.sort_values(by=[sort_col], ascending=[sort_asc], axis=0)

        # Optionally, format file size
        if format_size == True:
            df['size'] = df['size'].apply(lambda x: self._format_file_size(x))

        return df

    def file_download(self, source: str, target: str) -> None:
        """Download a file from the remote file system.

        Args:
            source (str): Relative file path on the remote file system.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        result = super().file_download(source, target)

        print('File {source} copied to {target} successfully'.format(
            source=source, target=target))

        return result

    def file_upload(self, source: str, target: str, overwrite: bool = False) -> None:
        """Upload a file to the remote file system.

        Note:
            File size constraints are enforced on the uploaded file. The default
            installation allows files up to 20 Mb. 

            Moreover, space quotas are applied on the server. The default user
            space is 5GB.

            Directory nesting constraints are applied for the ``target`` value. The
            default installation allows nesting of directories up to 5 levels.

        Args:
            source (str): The path of the file to upload.
            target (str): Relative path on the remote file system where to save the
                file. If the directory does not exist, it will be created.
            overwrite (bool, optional): Set true if the operation should overwrite
                any existing file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        super().file_upload(source, target, overwrite=overwrite)

        print('File uploaded successfully')

    def catalog_query(self, term: str = None, pageIndex: int = 0, pageSize: int = 10) -> pandas.DataFrame:
        """Query resource catalog for RDF datasets.

        Args:
            term (str, optional): A term for filtering resources. If specified, 
                only the resources whose name contains the term are returned.
            pageIndex (str, optional): Page index for data pagination.
            pageSize (str, optional): Page size for data pagination.

        Returns:
            A :obj:`pandas.DataFrame` with the selected resources 

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().catalog_query(
            term=term,
            pageIndex=pageIndex,
            pageSize=pageSize
        )

        return pandas.io.json.json_normalize(
            result,
            record_path=['items']
        ).drop(
            ['version'], axis=1
        )[['id', 'name', 'description', 'size', 'numberOfEntities', 'tableName', 'createdOn', 'boundingBox']]

    def catalog_download(self, resource_id: int, target: str) -> None:
        """Download resource to the local file system

        Args:
            resource_id (int): The resource id.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        result = self.catalog_client.download(
            resource_id, 1, target)

        print('Resource {resource} copied to {target} successfully'.format(
            resource=(resource_id, 1), target=target))

        return result

    def _process_to_dict(self, p: dict) -> dict:
        return {
            'id': p['id'],
            'version': p['version'],
            'updatedOn':  self._to_datetime(p['updatedOn']),
            'executedOn': self._to_datetime(p['executedOn']),
            'name': p['name'],
            'description': p['description'],
            'taskType': p['taskType'],
        }

    def _flatten_workflows(self, records: list, result: list) -> list:
        if not type(records) is list:
            return

        for p in records:
            if 'revisions' in p:
                for r in p['revisions']:
                    result.append(self._process_to_dict(r))
            else:
                result.append(self._process_to_dict(p))

        return result

    def process_query(self, term: str = None, pageIndex: int = 0, pageSize: int = 10) -> pandas.DataFrame:
        """Query workflow instances.

        Args:
            term (str, optional): A term for filtering workflows. If specified, 
                only the workflows whose name contains the term are returned.
            pageIndex (str, optional): Page index for data pagination (default `0`).
            pageSize (str, optional): Page size for data pagination (default `10`).

        Returns:
            A :obj:`pandas.DataFrame` with the selected processes

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().process_query(
            term=term,
            pageIndex=pageIndex,
            pageSize=pageSize
        )

        data = self._flatten_workflows(result['items'], [])

        return pandas.DataFrame(
            data=data
        )

    def _collect_process_execution_files(self, exec: dict) -> pandas.DataFrame:
        result = []

        if not type(exec) is dict or not 'steps' in exec:
            return result

        for s in exec['steps']:
            if type(s) is dict and 'files' in s:
                for f in s['files']:
                    result.append({
                        'id': f['id'],
                        'type': f['type'],
                        'step': s['name'],
                        'tool': s['tool'],
                        'name': f['name'],
                        'size': f['size'],
                    })

        return result

    def process_status(self, process_id: int, process_version: int, format_size: bool = False) -> pandas.DataFrame:
        """Check the status of a workflow execution instance and return all
        execution files.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.
            format_size (bool, optional): If true, file size is converted to a
                user friendly string (default `False`).

        Returns:
            A :obj:`pandas.DataFrame` with all execution files

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().process_status(process_id, process_version)

        # Extract files from execution
        data = self._collect_process_execution_files(result)

        df = pandas.DataFrame(data=data)

        # Sort by type and name
        df = df.sort_values(by=['type', 'id'], axis=0)

        # Optionally, format file size
        if format_size == True:
            df['size'] = df['size'].apply(lambda x: self._format_file_size(x))

        print('Process {process} status is {status}'.format(
            process=(process_id, process_version), status=result['status']))

        return df

    def process_start(self, process_id: int, process_version: int) -> None:
        """Start or resume execution of a workflow instance.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().process_start(process_id, process_version)

        print('Process {process} started successfully'.format(
            process=(process_id, process_version)))

        return result

    def process_stop(self, process_id: int, process_version: int) -> None:
        """Stop a running workflow execution instance.

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().process_stop(process_id, process_version)

        print('Process {process} stopped successfully'.format(
            process=(process_id, process_version)))

        return result

    def process_file_download(self, process_id: int, process_version: int, file_id: int, target: str) -> None:
        """Download an input or output file for a specific workflow execution instance.

        During the execution of a workflow the following file types may be created:
            - ``CONFIGURATION``: Tool configuration
            - ``INPUT``: Input file
            - ``OUTPUT``: Output file
            - ``SAMPLE``: Sample data collected during step execution
            - ``KPI``: Tool specific or aggregated KPI data
            - ``QA``: Tool specific QA data
            - ``LOG``: Logs recorded during step execution 

        Args:
            process_id (int): The process id.
            process_version (int): The process revision.
            file_id (int): The file id.
            target (str): The path where to save the file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        result = super().process_file_download(
            process_id, process_version, file_id, target)

        print('Process file {file} copied to {target} successfully'.format(
            file=(process_id, process_version, file_id), target=target))

        return result

    def _status_to_dataframe(self, result: dict) -> pandas.DataFrame:
        status = {
            'id': result['id'],
            'process_id': result['processId'],
            'process_version': result['processVersion'],
            'status': result['status'],
            'taskType': result['taskType'],
            'name': result['name'],
            'started_on': self._to_datetime(result['startedOn']),
            'completed_on': self._to_datetime(result['completedOn']),
        }

        return pandas.io.json.json_normalize(
            status,
        )[['id', 'process_id', 'process_version', 'status', 'taskType', 'name', 'started_on', 'completed_on']]

    def _handle_operation_response(self, result: dict) -> pandas.DataFrame:
        print('New process {process} status is {status}'.format(
            process=(result['processId'], result['processVersion']), status=result['status']))

        return self._status_to_dataframe(result)

    def transform_csv(
        self,
        path: str,
        profile: str,
        **kwargs
    ) -> pandas.DataFrame:
        """Transforms a CSV file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            **kwargs: Keyword arguments to control the transform operation. Options are:

                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - delimiter (str, optional): Specify the character delimiting attribute
                  values.
                - **quote** (str, optional): Specify quote character for string values.
                - **attrX** (str, optional): Specify attribute holding X-coordinates of
                  point locations.
                - **attrY** (str, optional): Specify attribute holding Y-coordinates of
                  point locations.
                - **sourceCRS** (str, optional): Specify the EPSG numeric code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG numeric code for the
                  target CRS (default: `EPSG:4326`).
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`).

        Returns:
            A :obj:`pandas.DataFrame` that contains a single row with execution details

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().transform_csv(
            path,
            profile,
            **kwargs
        )

        return self._handle_operation_response(result)

    def transform_shapefile(
        self,
        path: str,
        profile: str,
        **kwargs
    ) -> pandas.DataFrame:
        """Transforms a SHAPEFILE file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            **kwargs: Keyword arguments to control the transform operation. Options
                are:

                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - **sourceCRS** (str, optional): Specify the EPSG numeric code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG numeric code for the
                  target CRS (default: `EPSG:4326`).
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`)

        Returns:
            A :obj:`pandas.DataFrame` that contains a single row with execution details

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().transform_shapefile(
            path,
            profile,
            **kwargs
        )

        return self._handle_operation_response(result)

    def interlink(
        self,
        profile: str,
        left: InputType,
        right: InputType
    ) -> pandas.DataFrame:
        """Generates links for two RDF datasets.

        Arguments `left`, `right` and `links` may be either a :obj:`dict` or
        a :obj:`tuple` of two integer values. The former represents a relative
        path to the remote user file system, while the latter the id and revision
        of a catalog resource.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int]]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int]]): The `right` RDF dataset.

        Returns:
            A :obj:`pandas.DataFrame` that contains a single row with execution details

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = self.operation_client.interlink(profile, left, right)

        return self._handle_operation_response(result)

    def fuse(
        self,
        profile: str,
        left: InputType,
        right: InputType,
        links: InputType
    ) -> pandas.DataFrame:
        """Fuses two RDF datasets using Linked Data and returns a new RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int]]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int]]): The `right` RDF dataset.
            links (Union[str, Tuple[int, int]]): The links for the `left` and `right` datasets.

        Returns:
            A :obj:`pandas.DataFrame` that contains a single row with execution details

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().fuse(profile, left, right, links)

        return self._handle_operation_response(result)

    def enrich(
        self,
        profile: str,
        source: InputType
    ) -> pandas.DataFrame:
        """Enriches a RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            source (Union[str, Tuple[int, int]]): The RDF dataset to enrich.

        Returns:
            A :obj:`pandas.DataFrame` that contains a single row with execution details

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().enrich(profile, source)

        return self._handle_operation_response(result)
