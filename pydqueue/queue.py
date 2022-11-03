"""Module containing the queue class"""
from collections import Counter
from typing import List, Dict

from . import utils
from .task import Task, TaskFlag

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

    def check(self) -> bool:
        """Performs check, if queue is setup correctly"""
        task_names = [task.name for task in self.tasks if task.name is not None]
        if not all(v == 1 for v in Counter(task_names).values()):
            raise RuntimeError('All tasks must have different names!')

    def run(self, initial_input_data: Dict, **kwargs: Dict):
        """Running the queue"""
        verbose = kwargs.get('verbose', False)
        self.check()

        history = []
        ntasks = len(self.tasks)

        if verbose:
            print(f'>>> (1/{ntasks}) Run "{self.tasks[0].name}"')
        flag, output = self.tasks[0](None, initial_input_data, **kwargs)
        if verbose:
            print('    ...finished <<<')

        self.tasks[0].output = output
        for itask, task in enumerate(self.tasks[1:]):
            if verbose:
                print(f'>>> ({itask + 2}/{ntasks}) Run "{task}"')
            if task.has_parents:  # no parents
                for ptask in task.parents:
                    print(f'_> Try running from "{ptask.name}"')
                    if ptask.flag:
                        flag, output = task(ptask.flag, ptask.output, **kwargs)
                        break
            else:
                flag, output = task(flag, {}, **kwargs)
            task.output = output
            task.flag = TaskFlag(flag)
            if verbose:
                print('    ...finished <<<')

            history.append((flag, output))
        return history

    def report(self) -> None:
        """Print report about tasks
        TODO: add start time and end time as additional columns
        """
        first_column_length = max(len(task.name) for task in self.tasks) + 2
        print('------------\nQueue report\n------------')
        for task in self.tasks:
            if task.flag == TaskFlag.failed:
                task_str = utils.failtext(task.flag.name)
            elif task.flag == TaskFlag.succeeded:
                task_str = utils.oktext(task.flag.name)
            else:
                task_str = task.flag.name
            print(f'{task.name:>{first_column_length}}: {task_str:<18} '
                  f'({task.start_time:>} - {task.end_time:>})')
