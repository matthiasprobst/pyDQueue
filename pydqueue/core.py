"""Core module containin queing and task classes"""

from collections import Counter

import types
from datetime import datetime
from enum import Enum
from itertools import count
from typing import List, Dict

from . import utils

MAX_LINE_LENGTH = 123

task_id = count()
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


class TaskFlag(Enum):
    """Task flag"""
    not_started = -1
    failed = 0
    succeeded = 1
    error = 2
    none = 99


def get_time():
    """get the current time"""
    return datetime.now().strftime(DATETIME_FMT)


class Task:
    def __init__(self, obj, task_clsname):
        self.id = next(task_id)
        self._task_clsname = task_clsname
        self._name = f'{self._task_clsname}{self.id}'

        self.parents = []
        self._flag = TaskFlag.not_started
        self.output = None
        self._start_time = None
        self._end_time = None
        self._err_msg = None
        self._run = obj.run

    def __repr__(self) -> str:
        if self.error_message:
            return f'{self.name} (flag={self.flag}, err_msg={self.error_message})'
        return f'<{self.name} (flag={self.flag.name})>'

    @property
    def flag(self) -> TaskFlag:
        """Return the current flag value of the Task"""
        if isinstance(self._flag, int):
            return TaskFlag(self._flag)
        return self._flag

    @flag.setter
    def flag(self, flag: TaskFlag) -> None:
        """Set the current flag of the Task"""
        if isinstance(flag, int):
            self._flag = TaskFlag(flag)
        elif isinstance(flag, TaskFlag):
            self._flag = flag
        else:
            raise TypeError(f'Wrong flag type. Must be "int" or "TaskFlag", not {type(flag)}')

    @property
    def error_message(self) -> Exception:
        """Return the error"""
        return self._err_msg

    @property
    def start_time(self) -> str:
        """Time when task started"""
        return self._start_time

    @property
    def end_time(self) -> str:
        """Time when task finished or crashed"""
        return self._end_time

    @property
    def has_parents(self) -> bool:
        """If number of parents is unequal to zero"""
        return len(self.parents) > 0

    @property
    def name(self) -> str:
        """Return the task name. If self._name is None a name based on the task
        id is generated"""
        return self._name

    def run(self, *args, **kwargs):
        """Run the task"""
        print(f'task method input: {args}, {kwargs}')
        self._start_time = get_time()
        try:
            output = self._run(*args, **kwargs)
            self.flag = output.get('flag')
            self.output = output
            self._err_msg = None
        except Exception as e:
            self._err_msg = e
            self.flag = TaskFlag.error
            self.output = {}
        self._end_time = get_time()

    def add_parent(self, parent_task: "Task") -> None:
        """Add a parent task, from which the output is taken for input for this task.

        Parameters
        ----------
        parent_task: Task
            The task to take as input
        """
        if self.id == parent_task.id:
            raise RuntimeError('Cannot add a task to itself!')
        if self.id < parent_task.id and parent_task.has_parents:
            raise RuntimeError('A task added must has no parents or be computed before this task!')
        self.parents.append(parent_task)

    def remove_parent(self, index: int) -> None:
        """removes parent at index location in list of parents"""
        self.parents.pop(index)

    def remove_parent_by_name(self, parent_name: str) -> None:
        """removes parent at index location in list of parents"""
        for i, parent in self.parents:
            if parent.name == parent_name:
                self.parents.pop(i)
                return
        raise IndexError(f'Could not find parent with name {parent_name} in list of parents: '
                         f'{[p.name for p in self.parents]}')

    def add_parents(self, *parent_tasks: "Task") -> None:
        """Add multiple parent tasks"""
        if len(parent_tasks) == 1 and isinstance(parent_tasks[0], list):
            parent_tasks = parent_tasks[0]
        for parent_task in parent_tasks:
            self.add_parent(parent_task)


class TaskDecorator:

    def __init__(self, task, name):
        self.task = task
        self.name = name

    def __call__(self, *args, **kwargs):
        taskobj = self.task(*args, **kwargs)
        task_clsname = taskobj.__class__.__name__
        try:
            _task_method = callable(taskobj.__getattribute__('run'))
        except AttributeError:
            raise AttributeError(f'Class {self.run.__class__} has no method "run"')
        if not callable(taskobj.__getattribute__('run')):
            raise TypeError(f'Task seems not to be a method of {self.run.__class__}')
        return Task(taskobj, task_clsname)


# wrap Task to allow for deferred calling
def task(cls=None) -> TaskDecorator:
    """taskwrapper"""

    def wrapper(function):
        return TaskDecorator(function, 'Unknown')

    if isinstance(cls, types.FunctionType):
        # it's a function
        _name = cls.__name__

        class _Task:
            def run(self, *args, **kwargs):
                return cls(*args, **kwargs)

        _Task.__name__ = cls.__name__.capitalize()
        return TaskDecorator(_Task, cls.__name__.capitalize())

    if cls is None:
        return wrapper

    if not isinstance(cls, str):
        return TaskDecorator(cls, cls.__name__)
    else:
        return wrapper


class Queue:
    """Queue class"""

    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def __len__(self) -> int:
        """Number of tasks"""
        return len(self.tasks)

    def get_infostr(self, use_task_name: bool = False) -> str:
        """returns queue string"""
        _str = ''
        ntasks = self.__len__()
        _nlines = 1
        for itask, _task in enumerate(self.tasks):
            if use_task_name:
                _str += f'{_task.name}('
            else:
                _str += f'Task-{itask}('
            if _task.parents is not None:
                n_parents = len(_task.parents)
                for i_ptask, ptask in enumerate(_task.parents):
                    if use_task_name:
                        _str += f'{ptask.name}'
                    else:
                        _str += f'Task-{i_ptask}'
                    if 1 < n_parents and i_ptask != n_parents - 1:
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

    def info(self, use_task_name: bool = False):
        """prints queue"""
        print(self.get_infostr(use_task_name))

    def __repr__(self) -> str:
        return self.get_infostr(use_task_name=True)

    def __getitem__(self, item) -> Task:
        return self.tasks[item]

    def append(self, task: Task) -> None:
        """append a task"""
        self.tasks.append(task)

    def check(self) -> bool:
        """Performs check, if queue is setup correctly"""
        task_names = [_task.name for _task in self.tasks if _task.name is not None]
        if not all(v == 1 for v in Counter(task_names).values()):
            raise RuntimeError('All tasks must have different names!')

    def run(self, *args, **kwargs: Dict):
        """Running the queue"""

        verbose = kwargs.get('verbose', False)
        self.check()

        ntasks = len(self.tasks)

        for itask, _task in enumerate(self.tasks):

            if verbose:
                print(utils.oktext(utils.make_bold(f'\n>>> ({itask + 1}/{ntasks}) Run "{_task}"')))

            if itask == 0:
                # first _task can get initial input data
                flag = TaskFlag.none
            else:
                initial = {}

            # part with all tasks after the first one:
            if _task.has_parents:  # no parents
                all_parents_failed = True
                for parent_task in _task.parents:
                    if verbose:
                        print(f'_> Try running from "{parent_task.name}"')
                    if parent_task.flag == TaskFlag.succeeded:
                        if 'flag' not in parent_task.output:
                            parent_task['flag'] = parent_task.flag
                        _task.run(**parent_task.output, **kwargs)
                        all_parents_failed = False
                        break
                if all_parents_failed:
                    if verbose:
                        print(f'_> All parents failed for some reason')
                    flag = TaskFlag.failed
                    if 'flag' not in parent_task.output:
                        kwargs['flag'] = flag
                    _task.run(**kwargs)
                    _task._start_time = get_time()
            else:
                _task.run(flag, *args, **kwargs)

            if verbose:
                print(utils.oktext(utils.make_bold('    ...finished <<<')))

    def report(self) -> None:
        """Print report about tasks"""
        first_column_length = max(len(_task.name) for _task in self.tasks) + 2
        print('------------\nQueue report\n------------')
        for _task in self.tasks:
            if _task.flag == TaskFlag.failed or _task.flag == TaskFlag.error:
                task_str = utils.failtext(_task.flag.name)
            elif _task.flag == TaskFlag.succeeded:
                task_str = utils.oktext(_task.flag.name)
            else:
                task_str = _task.flag.name
            if _task.error_message is not None:
                print(f'{_task.name:>{first_column_length}}: {task_str:<18} '
                      f'({_task.start_time:>} - {_task.end_time:>}) err: {_task.error_message.__repr__()}')
            else:
                print(f'{_task.name:>{first_column_length}}: {task_str:<18} '
                      f'({_task.start_time:>} - {_task.end_time:>})')


class RQueue:
    """Special Queue "Recursive-Queue" (RQueue) where every task first takes the result
    from the task before and if that failed it takes the result from the task
    before that and so on. Using this class avoid defining parent tasks because they are automatically
    defined. On such use case is the call of CFD simulations of various operation points (e.g. the
    characteristic curve of a fan). In that case we would start with a simulation that converges or
    will finish successfully. Next would be a simulation case that is a bit more challenging for the CFD
    and would best start from the neighbouring operation point. But if that failed the one before that would
    be taken and so on."""
    # TODO
