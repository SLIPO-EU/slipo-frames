import pandas

from .utils import format_file_size, timestamp_to_datetime


class StepFile(object):
    """A class that wraps the :obj:`dict` with step file data returned by the API

    Args:
        process (dict): Parent process data
        step_file (dict): Step file data

    Returns:
        A :py:class:`StepFile <slipoframes.model.StepFile>` object.

    """

    def __init__(self, process: dict, step_file: dict):
        self.__process = process
        self.__file = step_file

    @property
    def id(self):
        return self.__file['id']

    @property
    def process_id(self):
        return self.__process['processId']

    @property
    def process_version(self):
        return self.__process['processVersion']

    @property
    def name(self):
        return self.__file['name']

    @property
    def output_type(self):
        return self.__file['type']

    @property
    def output_part_key(self):
        return self.__file['outputPartKey']

    @property
    def size(self):
        return self.__file['size']

    def __str__(self):
        return 'File ({id}, {name})'.format(id=self.id, name=self.name)

    def __repr__(self):
        return 'File ({id}, {name})'.format(id=self.id, name=self.name)


class Process(object):
    """A class that wraps the :obj:`dict` with process data returned by the API

    Args:
        process (dict): Process execution data

    Returns:
        A :py:class:`Process <slipoframes.model.Process>` object.

    """

    def __init__(self, process: dict):
        self.__process = process

    @property
    def id(self):
        return self.__process['processId']

    @property
    def version(self):
        return self.__process['processVersion']

    @property
    def status(self):
        return self.__process['status']

    @property
    def name(self):
        return self.__process['name']

    @property
    def submitted_on(self):
        return timestamp_to_datetime(self.__process['submittedOn'])

    @property
    def started_on(self):
        return timestamp_to_datetime(self.__process['startedOn'])

    @property
    def completedOn(self):
        return self.__process['completedOn']

    def steps(self):
        # Extract files from execution
        data = self._collect_process_execution_steps(self.__process)

        df = pandas.DataFrame(data=data)

        # Sort by name
        df = df.sort_values(by=['Name'], axis=0)

        # Reorder columns
        df = df[['Name', 'Tool', 'Operation',
                 'Status', 'Started On', 'Completed On']]

        return df

    def _collect_process_execution_steps(self, exec: dict) -> pandas.DataFrame:
        result = []

        if not type(exec) is dict or not 'steps' in exec:
            return result

        for s in exec['steps']:
            result.append({
                'Name': s['name'],
                'Tool': s['tool'],
                'Operation': s['operation'],
                'Status': s['status'],
                'Started On': timestamp_to_datetime(s['startedOn']) or '',
                'Completed On': timestamp_to_datetime(s['completedOn']) or '',
            })

        return result

    def files(self, format_size: bool = False):
        # Extract files from execution
        data = self._collect_process_execution_files(self.__process)

        df = pandas.DataFrame(data=data)

        # Sort by type and name
        df = df.sort_values(by=['Type', 'Id'], axis=0)

        # Optionally, format file size
        if format_size == True:
            df['Size'] = df['Size'].apply(lambda x: format_file_size(x))

        # Reorder columns
        df = df[['Id', 'Step', 'Tool', 'Type',
                 'Output Part Key', 'Name', 'Size']]

        return df

    def _collect_process_execution_files(self, exec: dict) -> pandas.DataFrame:
        result = []

        if not type(exec) is dict or not 'steps' in exec:
            return result

        for s in exec['steps']:
            if type(s) is dict and 'files' in s:
                for f in s['files']:
                    result.append({
                        'Id': f['id'],
                        'Type': f['type'],
                        'Output Part Key': f['outputPartKey'] or '',
                        'Step': s['name'],
                        'Tool': s['tool'],
                        'Name': f['name'],
                        'Size': f['size'],
                    })

        return result

    def output(self, output_part_key=None) -> StepFile:
        """Returns a :py:class:`StepFile <slipoframes.model.StepFile>`
        for the default process output.

        Args:
            output_part_key (str, optional): The output part key of the output file.
                If value is not set, the default output part key for the specific SLIPO
                Toolkit component is used.

        Returns:
            A :py:class:`StepFile <slipoframes.model.StepFile>`. If the process has
            multiple steps or the output part key does not exist, `None` is returned.
        """

        # Steps must exit
        if self.__process is None or not 'steps' in self.__process:
            return None

        steps = self.__process['steps']

        # Only operations with a single step are supported
        if not type(steps) is list or len(steps) != 1:
            return None

        step = steps[0]

        # Check files
        if not 'files' in step:
            return None

        # Resolve output key
        if output_part_key is None:
            tool = step['tool']

            if tool == 'TRIPLEGEO':
                output_part_key = 'transformed'
            elif tool == 'LIMES':
                output_part_key = 'accepted'
            elif tool == 'FAGI':
                output_part_key = 'fused'
            elif tool == 'DEER':
                output_part_key = 'enriched'
            elif tool == 'REVERSE_TRIPLEGEO':
                output_part_key = 'transformed'

        if output_part_key is None:
            return None

        files = step['files']

        matches = [f for f in files if f['outputPartKey'] == output_part_key]

        return StepFile(self.__process, matches[0]) if len(matches) == 1 else None

    def __str__(self):
        return 'Process ({id}, {version}) status is {status}'.format(id=self.id, version=self.version, status=self.status)

    def __repr__(self):
        return 'Process ({id}, {version})'.format(id=self.id, version=self.version)
