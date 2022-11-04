"""Module containing the queue class"""
from collections import Counter
from typing import List, Dict

from . import utils
from .task import Task, TaskFlag, get_time

MAX_LINE_LENGTH = 123


class Queue:
    """Queue class"""

    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def __len__(self) -> int:
        """Number of tasks"""
        return len(self.tasks)

    def __repr__(self) -> str:
        _str = ''
        ntasks = self.__len__()
        _nlines = 1
        for itask, task in enumerate(self.tasks):
            _str += f'{task.name}('
            if task.parents is not None:
                nparents = len(task.parents)
                for iptask, ptask in enumerate(task.parents):
                    _str += f'{ptask.name}'
                    if 1 < nparents and iptask != nparents - 1:
                        _str += ','
            if itask == ntasks - 1:
                _str += ')'
            else:
                _str += ')'

            if itask != ntasks - 1:
                _str += ' --> '
                if len(_str) > _nlines * MAX_LINE_LENGTH:
                    _nlines += 1
                    _str += '... \n ... --> '

        return _str

    def __getitem__(self, item) -> Task:
        return self.tasks[item]

    def check(self) -> bool:
        """Performs check, if queue is setup correctly"""
        task_names = [task.name for task in self.tasks if task.name is not None]
        if not all(v == 1 for v in Counter(task_names).values()):
            raise RuntimeError('All tasks must have different names!')

    def run(self, initial_input_data: Dict = None, **kwargs: Dict):
        """Running the queue"""
        if initial_input_data is None:
            initial_input_data = {}

        verbose = kwargs.get('verbose', False)
        self.check()

        ntasks = len(self.tasks)

        for itask, task in enumerate(self.tasks):

            if verbose:
                print(utils.oktext(utils.make_bold(f'\n>>> ({itask + 1}/{ntasks}) Run "{task}"')))

            if itask == 0:
                # first task can get initial input data
                input_data = initial_input_data
                flag = TaskFlag.none
            else:
                input_data = {}

            # part with all tasks after the first one:
            if task.has_parents:  # no parents
                all_parents_failed = True
                for parent_task in task.parents:
                    if verbose:
                        print(f'_> Try running from "{parent_task.name}"')
                    if parent_task.flag == TaskFlag.succeeded:
                        task(parent_task.flag, parent_task.output, **kwargs)
                        all_parents_failed = False
                if all_parents_failed:
                    if verbose:
                        print(f'_> All parents failed for some reason')
                    flag = TaskFlag.failed
                    task(flag, input_data, **kwargs)
                    task._start_time = get_time()
            else:
                task(flag, input_data, **kwargs)

            if verbose:
                print(utils.oktext(utils.make_bold('    ...finished <<<')))

    def report(self) -> None:
        """Print report about tasks"""
        first_column_length = max(len(task.name) for task in self.tasks) + 2
        print('------------\nQueue report\n------------')
        for task in self.tasks:
            if task.flag == TaskFlag.failed or task.flag == TaskFlag.error:
                task_str = utils.failtext(task.flag.name)
            elif task.flag == TaskFlag.succeeded:
                task_str = utils.oktext(task.flag.name)
            else:
                task_str = task.flag.name
            if task.error_message is not None:
                print(f'{task.name:>{first_column_length}}: {task_str:<18} '
                      f'({task.start_time:>} - {task.end_time:>}) err_msg: {task.error_message}')
            else:
                print(f'{task.name:>{first_column_length}}: {task_str:<18} '
                      f'({task.start_time:>} - {task.end_time:>})')
