"""
SLIPO API client wrapper.

This class provides methods for accessing `SLIPO API` functionality
from Jupyter notebooks
"""

import os
import math
import pandas

from typing import Union, Tuple
from datetime import datetime

from slipo.client import Client
from slipo.exceptions import SlipoException

from .model import Process, StepFile
from .utils import timestamp_to_datetime, format_file_size

InputType = Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]

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

        self.validate()

        print('Application key is valid!')

    def _flatten_file_system(self, obj: dict, result: list) -> list:
        if not type(obj) is dict:
            return

        if 'files' in obj:
            for f in obj['files']:
                result.append({
                    'name': f['name'],
                    'modified': timestamp_to_datetime(f['modified']),
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

        # Check for empty frame
        if df.empty:
            print('No data found')
            return None

        # Check if sort column exists
        if not sort_col in df:
            print('Column {sort_col} was not found.'
                  ' Sorting by column modified'.format(sort_col=sort_col))
            sort_col = 'modified'

        df = df.sort_values(by=[sort_col], ascending=[sort_asc], axis=0)

        # Optionally, format file size
        if format_size == True:
            df['size'] = df['size'].apply(lambda x: format_file_size(x))

        return df

    def file_download(self, source: str, target: str, overwrite: bool = False) -> None:
        """Download a file from the remote file system.

        Args:
            source (str): Relative file path on the remote file system.
            target (str): The path where to save the file.
            overwrite (bool, optional): Set true if the operation should
                overwrite any existing file.

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        result = super().file_download(source, target, overwrite=overwrite)

        print('File {source} copied to {target} successfully'.format(
            source=source, target=target))

        return result

    def file_upload(self, source: str, target: str, overwrite: bool = False) -> None:
        """Upload a file or folder to the remote file system.

        When a folder is being copied, only the files of the folder are copied while
        any sub-folders are ignored.

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

        if os.path.isfile(source):
            # Copy a single file
            super().file_upload(source, target, overwrite=overwrite)

            print(
                'File [{source}] uploaded successfully'
                .format(source=source)
            )
        elif os.path.isdir(source):
            # Check target path
            extension = os.path.splitext(target)
            # The target folder must not have an extension
            if extension[1]:
                raise SlipoException(
                    'Target {target} must be a folder'.format(target=target))
            else:
                self._upload_folder(source, target, overwrite)
        else:
            raise SlipoException(
                'Source {source} does not exist'.format(source=source))

    def _upload_folder(self, sourceDir, targetDir, overwrite):
        # Copy all files from the source directory
        files = [
            f for f in os.listdir(sourceDir) if os.path.isfile(os.path.join(sourceDir, f))
        ]
        for f in files:
            sourceFile = os.path.join(sourceDir, f)
            targetFile = os.path.join(targetDir, f)

            super().file_upload(sourceFile, targetFile, overwrite=overwrite)

            print(
                'File [{targetFile}] uploaded successfully'
                .format(targetFile=targetFile)
            )

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
            'Id': p['id'],
            'Version': p['version'],
            'Updated On':  timestamp_to_datetime(p['updatedOn']),
            'Executed On': timestamp_to_datetime(p['executedOn']),
            'Name': p['name'],
            'Description': p['description'],
            'Task Type': p['taskType'],
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

        df = pandas.DataFrame(data=data)

        # Check for empty frame
        if df.empty:
            print('No data found')
            return None

        # Reorder columns
        df = df[['Id', 'Version', 'Task Type', 'Name',
                 'Description', 'Updated On', 'Executed On']]

        return df

    def process_status(self, process_id: Union[int, Process], process_version: int = None, format_size: bool = False) -> Process:
        """Check the status of a workflow execution instance and return all
        execution files.

        Args:
            process_id (Union[int, Process]): The process id or an instance of
                :py:class:`Process <slipoframes.model.Process>`
            process_version (int): The process revision. If `process_id` is an
                instance of :py:class:`Process <slipoframes.model.Process>`, this
                parameter is ignored.
            format_size (bool, optional): If true, file size is converted to a
                user friendly string (default `False`).

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = None
        if type(process_id) is Process:
            result = super().process_status(process_id.id, process_id.version)
        else:
            result = super().process_status(process_id, process_version)

        process = Process(result)

        print(process)

        return process

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

    def process_file_download(self, process_id: Union[int, StepFile], process_version: int = None, file_id: int = None, target: str = None, overwrite: bool = False) -> None:
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
            process_id (Union[int, StepFile]): The process id or an instance of 
                :py:class:`StepFile <slipoframes.model.StepFile>`. If a file object
                is specified, `process_version` and `file_id` arguments are ignored.
            process_version (int, optional): The process revision.
            file_id (int, optional): The file id.
            target (str): The path where to save the file.
            overwrite (bool, optional): Set true if the operation should overwrite
                any existing file.                

        Raises:
            SlipoException: If a network, server error or I/O error has occurred.
        """

        result = None

        if target is None:
            raise SlipoException('Parameter target is required')

        if os.path.isfile(target) and not overwrite:
            raise SlipoException(
                'File {target} already exists'.format(target=target))

        if os.path.isdir(target):
            raise SlipoException(
                'Parameter {target} is a directory'.format(target=target))

        if type(process_id) is StepFile:
            result = super().process_file_download(
                process_id.process_id, process_id.process_version, process_id.id, target)
        else:
            result = super().process_file_download(
                process_id, process_version, file_id, target)

        if type(process_id) is StepFile:
            print('Process file {file} copied to {target} successfully'.format(
                file=(process_id.process_id,  process_id.process_version, process_id.id), target=target))
        else:
            print('Process file {file} copied to {target} successfully'.format(
                file=(process_id, process_version, file_id), target=target))

        return result

    def _handle_operation_response(self, result: dict) -> Process:
        process = Process(result)

        print(process)

        return process

    def profiles(self) -> pandas.DataFrame:
        """Get the profiles of all SLIPO Toolkit components.

        Returns:
            A :obj:`pandas.DataFrame` with the profiles of all SLIPO Toolkit components. 

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().profiles()

        data = []

        for tool in result.keys():
            for profile in result[tool]:
                data.append([tool, profile])

        df = pandas.DataFrame(data=data, columns=['Tool', 'Profile'])

        if not df.empty:
            df = df.sort_values(by=['Tool', 'Profile'], axis=0)

        return df

    def transform_csv(
        self,
        path: str,
        **kwargs
    ) -> Process:
        """Transforms a CSV file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            **kwargs: Keyword arguments to control the transform operation. Options are:

                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **attrX** (str, optional): Specify attribute holding X-coordinates of
                  point locations. If inputFormat is not `CSV`, the parameter is ignored.
                - **attrY** (str, optional): Specify attribute holding Y-coordinates of
                  point locations. If inputFormat is not `CSV`, the parameter is ignored.
                - **classificationSpec** (str, optional): The relative path to a YML/CSV
                  file describing a classification scheme.
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`).
                - delimiter (str, optional): Specify the character delimiting attribute
                  values.
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **mappingSpec** (str, optional): The relative path to a YML file containing
                  mappings from input schema to RDF according to a custom ontology.
                - **profile** (str, optional): The name of the profile to use. Profile names can
                  be retrieved using :meth:`profiles` method. If profile is not set, the
                  `mappingSpec` parameter must be set.
                - **quote** (str, optional): Specify quote character for string values.
                - **sourceCRS** (str, optional): Specify the EPSG code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG code for the
                  target CRS (default: `EPSG:4326`).

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().transform_csv(
            path,
            **kwargs
        )

        return self._handle_operation_response(result)

    def transform_shapefile(
        self,
        path: str,
        **kwargs
    ) -> Process:
        """Transforms a SHAPEFILE file to a RDF dataset.

        Args:
            path (str): The relative path for a file on the remote user file
                system.
            **kwargs: Keyword arguments to control the transform operation. Options
                are:

                - **attrCategory** (str, optional): Field name containing literals regarding
                  classification into categories (e.g., type of points, road classes etc.)
                  for each feature.
                - **attrGeometry** (str, optional): Parameter that specifies the name of the
                  geometry column in the input dataset.
                - **attrKey** (str, optional): Field name containing unique identifier for each
                  entity (e.g., each record in the shapefile).
                - **attrName** (str, optional): Field name containing name literals
                  (i.e., strings).
                - **classificationSpec** (str, optional): The relative path to a YML/CSV
                  file describing a classification scheme.
                - **defaultLang** (str, optional): Default lang for the labels created
                  in the output RDF (default: `en`).
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **featureSource** (str, optional): Specifies the data source provider of the
                  input features.
                - **mappingSpec** (str, optional): The relative path to a YML file containing
                  mappings from input schema to RDF according to a custom ontology.
                - **profile** (str, optional): The name of the profile to use. Profile names can
                  be retrieved using :meth:`profiles` method. If profile is not set, the
                  `mappingSpec` parameter must be set.
                - **sourceCRS** (str, optional): Specify the EPSG code for the
                  source CRS (default: `EPSG:4326`).
                - **targetCRS** (str, optional): Specify the EPSG code for the
                  target CRS (default: `EPSG:4326`).

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        result = super().transform_shapefile(
            path,
            **kwargs
        )

        return self._handle_operation_response(result)

    def interlink(
        self,
        profile: str,
        left: InputType,
        right: InputType
    ) -> Process:
        """Generates links for two RDF datasets.

        Arguments `left`, `right` and `links` may be either:

          - A :obj:`str` that represents a relative path to the remote user file system
          - A :obj:`tuple` of two integer values that represents the id and revision
            of a catalog resource.
          - A :obj:`tuple` of three integer values that represents the process id,
            process revision and output file id for a specific workflow or SLIPO API
            operation execution.
          - A :py:class:`StepFile <slipoframes.model.StepFile>` that represents the
            output of an operation.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The `right` RDF dataset.

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        if type(left) is StepFile:
            left = (left.process_id, left.process_version, left.id)

        if type(right) is StepFile:
            right = (right.process_id, right.process_version, right.id)

        result = self.operation_client.interlink(profile, left, right)

        return self._handle_operation_response(result)

    def fuse(
        self,
        profile: str,
        left: InputType,
        right: InputType,
        links: InputType
    ) -> Process:
        """Fuses two RDF datasets using Linked Data and returns a new RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            left (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The `left` RDF dataset.
            right (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The `right` RDF dataset.
            links (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The links for the `left` and `right` datasets.

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        if type(left) is StepFile:
            left = (left.process_id, left.process_version, left.id)

        if type(right) is StepFile:
            right = (right.process_id, right.process_version, right.id)

        if type(links) is StepFile:
            links = (links.process_id, links.process_version, links.id)

        result = super().fuse(profile, left, right, links)

        return self._handle_operation_response(result)

    def enrich(
        self,
        profile: str,
        source: InputType
    ) -> Process:
        """Enriches a RDF dataset.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            source (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The RDF dataset to enrich.

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        if type(source) is StepFile:
            source = (source.process_id, source.process_version, source.id)

        result = super().enrich(profile, source)

        return self._handle_operation_response(result)

    def export_csv(
        self,
        profile: str,
        source: InputType,
        **kwargs
    ) -> Process:
        """Exports a RDF dataset to a CSV file.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            source (Union[str, Tuple[int, int], Tuple[int, int, int]): The RDF dataset
                to export.
            **kwargs: Keyword arguments to control the transform operation. Options are:

                - **defaultLang** (str, optional): The default language for labels created
                  in output RDF. The default is "en".
                - delimiter (str, optional):A field delimiter for records (default: `;`).
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **quote** (str, optional): Specify quote character for string values (default `"`).
                - **sourceCRS** (str, optional): Specify the EPSG code for the
                  source CRS (default: `EPSG:4326`).
                - **sparqlFile** (str, optional): The relative path to a file containing a 
                  user-specified SELECT query (in SPARQL) that will retrieve results from
                  the input RDF triples. This query should conform with the underlying ontology
                  of the input RDF triples.
                - **targetCRS** (str, optional): Specify the EPSG code for the
                  target CRS (default: `EPSG:4326`).

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """
        if type(source) is StepFile:
            source = (source.process_id, source.process_version, source.id)

        result = super().export_csv(profile, source, **kwargs)

        return self._handle_operation_response(result)

    def export_shapefile(
        self,
        profile: str,
        source: InputType,
        **kwargs
    ) -> Process:
        """Exports a RDF dataset to a SHAPEFILE file.

        Args:
            profile (str): The name of the profile to use. Profile names can
                be retrieved using :meth:`profiles` method.
            source (Union[str, Tuple[int, int], Tuple[int, int, int], StepFile]): The RDF dataset
                to export.
            **kwargs: Keyword arguments to control the transform operation. Options
                are:

                - **defaultLang** (str, optional): The default language for labels created
                  in output RDF. The default is "en".
                - delimiter (str, optional):A field delimiter for records (default: `;`).
                - **encoding** (str, optional): The encoding (character set) for strings in the
                  input data (default: `UTF-8`)
                - **quote** (str, optional): Specify quote character for string values (default `"`).
                - **sourceCRS** (str, optional): Specify the EPSG code for the
                  source CRS (default: `EPSG:4326`).
                - **sparqlFile** (str, optional): The relative path to a file containing a 
                  user-specified SELECT query (in SPARQL) that will retrieve results from
                  the input RDF triples. This query should conform with the underlying ontology
                  of the input RDF triples.
                - **targetCRS** (str, optional): Specify the EPSG code for the
                  target CRS (default: `EPSG:4326`).

        Returns:
            A new instance of :py:class:`Process <slipoframes.model.Process>` class.

        Raises:
            SlipoException: If a network or server error has occurred.
        """

        if type(source) is StepFile:
            source = (source.process_id, source.process_version, source.id)

        result = super().export_shapefile(profile, source, **kwargs)

        return self._handle_operation_response(result)
