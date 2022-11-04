"""Module containing the task class"""

from abc import abstractmethod
from datetime import datetime
from enum import Enum
from itertools import count
from typing import Callable
from typing import Dict, Tuple, Union


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
    return datetime.now().strftime(DATETIME_FMT)


class TaskCaller:

    def __init__(self, task_specific_data: Dict):
        self.task_specific_data = task_specific_data

    def __call__(self, flag: Union[bool, None],
                 input_data: Dict, **kwargs):
        kwargs.update(input_data)
        return self.function(
            flag,
            input_data,
            **kwargs
        )

    @abstractmethod
    def function(self, flag, input_data, *args, **kwargs):
        """Function to be overwritten by the user in the subclass"""


class Task:
    """Task class"""

    def __init__(self, function: Callable, name: str = None):
        self.id = next(task_id)
        self.parents = []
        self._flag = TaskFlag.not_started
        self.output = None

        self._name = name
        self._function = function
        self._start_time = None
        self._end_time = None
        self._err_msg = None

    def __call__(self, parent_success: Union[bool, None],
                 input_data: Dict, **kwargs) -> Tuple[TaskFlag, Dict]:
        self._start_time = get_time()
        try:
            flag, output = self._function(parent_success, input_data, **kwargs)
            err_msg = None
        except Exception as e:
            err_msg = e
            flag = TaskFlag.error
            output = {}
        self._end_time = get_time()

        self.flag = flag
        self.output = output
        self._err_msg = err_msg

    def __repr__(self) -> str:
        if self.error_message:
            return f'{self.name} (flag={self.flag}, err_msg={self.error_message})'
        return f'{self.name} (flag={self.flag})'

    @property
    def flag(self) -> TaskFlag:
        if isinstance(self._flag, int):
            return TaskFlag(self._flag)
        return self._flag

    @flag.setter
    def flag(self, flag: TaskFlag) -> None:
        if isinstance(flag, int):
            self._flag = TaskFlag(flag)
        elif isinstance(flag, TaskFlag):
            self._flag = flag
        else:
            raise TypeError(f'Wrong flag type. Must be "int" or "TaskFlag", not {type(flag)}')

    @property
    def error_message(self) -> str:
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
        if self._name is None:
            return f'Task{self.id}'
        return self._name

    def add_parent(self, parent_task: "Task") -> None:
        """Add a parent task whichs output is taken for input

        Parameters
        ----------
        parent_task: Task
            The task to take as input
        """
        for parent in self.parents:
            if parent_task == parent:
                raise ValueError(f'Task "{parent_task}" already exists as parent task!')
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
        for parent_task in parent_tasks:
            self.add_parent(parent_task)
