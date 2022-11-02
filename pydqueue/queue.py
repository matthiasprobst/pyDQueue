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
        for ib, b in enumerate(self.tasks):
            _str += f'{b.name}('
            if b.parents is not None:
                nparents = len(b.parents)
                for ip, p in enumerate(b.parents):
                    _str += f'{p.name}'
                    if 1 < nparents and ip != nparents - 1:
                        _str += ','
            if ib == ntasks - 1:
                _str += ')'
            else:
                _str += ')'

            if ib != ntasks - 1:
                _str += ' --> '
                if len(_str) > _nlines * MAX_LINE_LENGTH:
                    _nlines += 1
                    _str += '... \n ... --> '

        return _str

    def check(self) -> bool:
        """Performs check, if queue is setup correctly"""
        task_names = [b.name for b in self.tasks if b.name is not None]
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
            print(f'    ...finished <<<')

        self.tasks[0].output = output
        for ib, b in enumerate(self.tasks):
            if verbose:
                print(f'>>> ({ib + 1}/{ntasks}) Run "{b}"')
            if b.has_parents:  # no parents
                for osp in b.parents:  # osp=on_success_parent
                    print(f'_> Try running from "{osp.name}"')
                    if osp.success:
                        success, output = b.function(osp.success, osp.output, **kwargs)
                        break
            else:
                success, output = b.function(success, {}, **kwargs)
            b.output = output
            b.success = success
            if verbose:
                print(f'    ...finished <<<')

            # success, output = b.function(success, output)
            history.append((success, output))
        return history
