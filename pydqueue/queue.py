"""Module containing the queue class"""
from collections import Counter
from typing import List, Dict

from .task import Task

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
        success, output = self.tasks[0].function(None, initial_input_data, **kwargs)
        if verbose:
            print('    ...finished <<<')

        self.tasks[0].output = output
        for itask, task in enumerate(self.tasks):
            if verbose:
                print(f'>>> ({itask + 1}/{ntasks}) Run "{task}"')
            if task.has_parents:  # no parents
                for ptask in task.parents:
                    print(f'_> Try running from "{ptask.name}"')
                    if ptask.success:
                        success, output = task.function(ptask.success, ptask.output, **kwargs)
                        break
            else:
                success, output = task.function(success, {}, **kwargs)
            task.output = output
            task.success = success
            if verbose:
                print('    ...finished <<<')

            history.append((success, output))
        return history
