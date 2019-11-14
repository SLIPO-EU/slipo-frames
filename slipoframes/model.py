import pandas

from .utils import format_file_size, timestamp_to_datetime


class StepFile(object):
    """A class that wraps the :obj:`dict` with step file data returned by the API

    Args:
        process (dict): Parent process data
        execution (dict): Process execution instance
        step_file (dict): Step file data

    Returns:
        A :py:class:`StepFile <slipoframes.model.StepFile>` object.

    """

    def __init__(self, process: dict, execution: dict, step_file: dict):
        self.__process = process
        self.__execution = execution
        self.__file = step_file

    @property
    def id(self):
        """Get step file unique id"""

        return self.__file['id']

    @property
    def process_id(self):
        return self.__process['id']

    @property
    def process_version(self):
        return self.__process['version']

    @property
    def name(self):
        return self.__file['name']

    @property
    def output_type(self):
        return self.__file['type']

    @property
    def output_part_key(self):
        """Get step file output part key"""

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
        record (dict): Process execution data

    Returns:
        A :py:class:`Process <slipoframes.model.Process>` object.

    """

    def __init__(self, record: dict):
        self.__process = record['process']
        self.__execution = record['execution'] or None

    @property
    def process(self):
        """Get process dict"""

        return self.__process

    @property
    def execution(self):
        """Get execution dict"""

        return self.__execution

    @property
    def id(self):
        """Get process unique id"""

        return self.__process['id']

    @property
    def version(self):
        """Get process version"""

        return self.__process['version']

    @property
    def status(self):
        """Get process status"""

        return 'UNKNOWN' if not self.__execution else self.__execution['status']

    @property
    def name(self):
        return self.__process['name']

    @property
    def submitted_on(self):
        return None if not self.__execution else timestamp_to_datetime(self.__execution['submittedOn'])

    @property
    def started_on(self):
        return None if not self.__execution else timestamp_to_datetime(self.__execution['startedOn'])

    @property
    def completedOn(self):
        return None if not self.__execution else self.__execution['completedOn']

    def steps(self):
        if self.__execution is None:
            return None

        # Extract files from execution
        data = self._collect_process_execution_steps(self.__execution)

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
        """Get all operation files

        Args:
            format_size (bool, optional): If `True`, the file size is converted
                to a user friendly string (default `False`).

        Returns:
            A :obj:`pandas.DataFrame` with all files 
        """

        if self.__execution is None:
            return None

        # Extract files from execution
        data = self._collect_process_execution_files(self.__execution)

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
            multiple output steps or the output part key does not exist, `None` is
            returned.
        """

        # Steps must exit
        if self.__execution is None or not 'steps' in self.__execution:
            return None

        # Get output step execution
        step = self.__get_output_step_execution()

        # Check files
        if step is None or not 'files' in step:
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

        return StepFile(self.__process, self.__execution, matches[0]) if len(matches) == 1 else None

    def __str__(self):
        return 'Process ({id}, {version}) status is {status}'.format(id=self.id, version=self.version, status=self.status)

    def __repr__(self):
        return 'Process ({id}, {version})'.format(id=self.id, version=self.version)

    def __get_output_step_execution(self):
        d = self.__process
        e = self.__execution

        inputs = set(
            [item for step in d['steps']
                for item in step['inputKeys'] if not item is None]
        )

        output = set(
            [step['outputKey']
                for step in d['steps'] if step['operation'] != 'REGISTER']
        )

        result = list(output - inputs)

        # Only executions with a single output step are supported
        if len(result) != 1:
            return None

        steps = [
            step for step in d['steps'] if step['outputKey'] == result[0]
        ]

        # Check number of steps since set operations may have removed duplicates.
        # Export steps have None as output key and more than one export steps may exist.
        if len(steps) != 1:
            return None

        return next(iter([step for step in e['steps'] if step['key'] == steps[0]['key']]), None)
