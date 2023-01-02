"""Core module containin queing and task classes"""
from collections import Counter

import copy
import inspect
import types
from datetime import datetime
from enum import Enum
from itertools import count
from typing import List, Dict, Callable

from . import utils

MAX_LINE_LENGTH = 123

TASK_COUNTER = count()
DATETIME_FMT = '%Y-%m-%d %H:%M:%S'


def qprint(string):
    print(f'<q> {string}')


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

    def __init__(self, run: Callable, task_clsname, id=None):
        if id is None:
            self.id = next(TASK_COUNTER)
        else:
            self.id = id
        self._task_clsname = task_clsname
        self._name = f'{self._task_clsname}<{self.id}>'
        self._run = run

    def __repr__(self) -> str:
        return f'<{self.name}>'

    @property
    def name(self) -> str:
        """Return the task name. If self._name is None a name based on the task
        id is generated"""
        return self._name

    def copy(self):
        return copy.copy(self)


class TaskDecorator:

    def __init__(self, task, name=None):
        self.task = task
        if name is None:
            self.name = task.__name__
        else:
            self.name = name
        try:
            _ = self.task.run
        except AttributeError:
            raise AttributeError(f'Class {self.__class__} has no method "run"')

    def __call__(self, *args, **kwargs):
        taskobj = self.task(*args, **kwargs)
        task_clsname = taskobj.__class__.__name__
        try:
            _task_method = callable(taskobj.__getattribute__('run'))
        except AttributeError:
            raise AttributeError(f'Class {self.__class__} has no method "run"')
        if not callable(taskobj.__getattribute__('run')):
            raise TypeError(f'Task seems not to be a method of {self.run.__class__}')
        return Task(taskobj.run, task_clsname)

    def __str__(self):
        return f'<Task "{self.name}">'


# wrap Task to allow for deferred calling
def task(cls=None) -> TaskDecorator:
    """taskwrapper"""

    def wrapper(function):
        return TaskDecorator(function, 'Unknown')

    if isinstance(cls, types.FunctionType):
        # it's a function
        _name = cls.__name__

        class _Task:
            def __init__(self, *args, **kwargs):
                pass

            def run(self, *args, **kwargs):
                if 'verbose' in kwargs:
                    if 'verbose' not in inspect.getfullargspec(cls).args:
                        # remove verbose from kwargs
                        _ = kwargs.pop('verbose', None)
                return cls(*args, **kwargs)

        _Task.__name__ = cls.__name__.capitalize()
        return TaskDecorator(_Task,
                             cls.__name__.capitalize())

    if cls is None:
        return wrapper

    if not isinstance(cls, str):
        return TaskDecorator(cls, cls.__name__)
    else:
        return wrapper


class Queue:
    """Queue class"""

    def __init__(self, tasks: List[Task] = None):
        global TASK_COUNTER
        TASK_COUNTER = count()  # reset counter
        if tasks is None:
            self.tasks = []
        else:
            self.tasks = [QTask(t, self) for t in tasks]

    def __len__(self) -> int:
        """Number of tasks"""
        return len(self.tasks)

    def get_infostr(self, use_task_name: bool = False) -> str:
        """returns queue string"""
        _str = ''
        ntasks = self.__len__()
        if ntasks == 0:
            return '<Empty Queue>'
        _nlines = 1
        for itask, _task in enumerate(self.tasks):
            if use_task_name:
                _str += f'{_task.name}('
            else:
                _str += f'Task<{itask}>('
            if _task.parents is not None:
                n_parents = len(_task.parents)
                for i_ptask, ptask in enumerate(_task.parents):
                    if use_task_name:
                        _str += f'{ptask.name}'
                    else:
                        _str += f'Task<{ptask.id}>'
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

    def __getitem__(self, item) -> "QTask":
        return self.tasks[item]

    def append(self, task: Task) -> None:
        """append a task"""
        self.tasks.append(QTask(task, self))

    def check(self) -> bool:
        """Performs check, if queue is setup correctly"""
        task_names = [_task.name for _task in self.tasks if _task.name is not None]
        if not all(v == 1 for v in Counter(task_names).values()):
            raise RuntimeError('All tasks must have different names!')

    def run(self, *, initial: Dict = None, stop_queue_on_error: bool = False, **kwargs: Dict):
        """Running the queue. Require keyword arguments.

        Parameters
        ----------
        initial: Dict=None
            Initial keyword arguments to be passed to the first task only
        stop_queue_on_error: bool=False
            The default (False) will let the queue run through all tasks. Errors will be
            registered but will not lead to a stop of the queue. If stop_queue_on_error is
            True the opposite will happen - the queue will stop on an error.
        """

        verbose = kwargs.get('verbose', False)
        self.check()

        ntasks = len(self.tasks)

        if initial is None:
            initial = {}

        if verbose:
            qprint(f'Starting Queue with {ntasks} tasks')
            qprint(f'Initial keyword arguments: {initial}')

        for itask, _task in enumerate(self.tasks):

            if verbose:
                qprint(utils.oktext(utils.make_bold(f'\n>>> ({itask + 1}/{ntasks}) Run "{_task}"')))

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
                        qprint(f'Try running from "{parent_task.name}"')
                    if parent_task.flag == TaskFlag.succeeded:
                        if isinstance(parent_task.output, dict):
                            _task.run(**parent_task.output,
                                      **initial,
                                      stop_queue_on_error=stop_queue_on_error,
                                      **kwargs)
                        else:
                            _task.run(parent_task.output, **initial, stop_queue_on_error=stop_queue_on_error, **kwargs)
                        all_parents_failed = False
                        break
                if all_parents_failed:
                    if verbose:
                        qprint(f'_> All parents failed for some reason')
                    flag = TaskFlag.failed
                    if 'flag' not in parent_task.output:
                        kwargs['flag'] = flag
                    _task.run(**initial, stop_queue_on_error=stop_queue_on_error, **kwargs)
                    _task._start_time = get_time()
            else:
                if verbose:
                    qprint(f'Task {_task} has no parent')
                _task.run(**initial, stop_queue_on_error=stop_queue_on_error, **kwargs)

            if verbose:
                qprint(utils.oktext(utils.make_bold('    ...finished <<<')))

    def report(self) -> None:
        """Print report about tasks"""
        first_column_length = max(len(_task.name) for _task in self.tasks) + 2
        qprint('------------')
        qprint('Queue report')
        qprint('------------')
        for _task in self.tasks:
            if _task.flag == TaskFlag.failed or _task.flag == TaskFlag.error:
                task_str = utils.failtext(_task.flag.name)
            elif _task.flag == TaskFlag.succeeded:
                task_str = utils.oktext(_task.flag.name)
            else:
                task_str = _task.flag.name
            if _task.error_message is not None:
                qprint(f'{_task.name:>{first_column_length}}: {task_str:<18} '
                       f'({_task.start_time:>} - {_task.end_time:>}) err: {_task.error_message.__repr__()}')
            else:
                qprint(f'{_task.name:>{first_column_length}}: {task_str:<18} '
                       f'({_task.start_time:>} - {_task.end_time:>})')


class QTask(Task):
    """Wrapper around task available inside a queue class"""

    def __init__(self, _task: Task, _queue: Queue):
        super().__init__(_task._run, _task._task_clsname, id=_task.id)
        self.parents = []
        self._flag = TaskFlag.not_started
        self.output = None
        self._start_time = None
        self._end_time = None
        self._err_msg = None
        self._queue = _queue

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

    def run(self, *args, **kwargs):
        """Run the task"""
        stop_queue_on_error = kwargs.pop('stop_queue_on_error', False)
        qprint(f'task method input: {kwargs}')
        self._start_time = get_time()
        try:
            output = self._run(*args, **kwargs)
            # if we come here, the above run succeeded
            self.flag = TaskFlag.succeeded
            self.output = output
            self._err_msg = None
        except Exception as e:
            if stop_queue_on_error:
                raise Exception(e)
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
        if parent_task not in self._queue.tasks:
            raise KeyError(f'Task {parent_task} not in queue: {self._queue.tasks}')
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
