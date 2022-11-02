"""Module containing the task class"""

from datetime import datetime
from enum import Enum
from itertools import count
from typing import Callable, Dict, Tuple, Union

task_id = count()
DATETIME_STR = '%Y-%m-%d %H:%M:%S'


class TaskFlag(Enum):
    """root attribute: data_source_type"""
    not_started = -1
    failed = 0
    succeeded = 1


class Task:
    """Task class"""

    def __init__(self, function: Callable, name: str = None):
        self.id = next(task_id)
        self.parents = []
        self.flag = TaskFlag.not_started
        self.output = None

        self._name = name
        self._function = function
        self._start_time = None
        self._end_time = None

    def __call__(self, parent_success: Union[bool, None],
                 input_data: Dict, **kwargs) -> Tuple[TaskFlag, Dict]:
        self._start_time = datetime.now().strftime(DATETIME_STR)
        flag, output = self._function(parent_success, input_data, **kwargs)
        self._end_time = datetime.now().strftime(DATETIME_STR)
        return TaskFlag(flag), output

    def __repr__(self) -> str:
        return f'{self.name}'

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
        return len(self.parents)

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

    def add_parents(self, *parent_tasks: "Task") -> None:
        """Add multiple parent tasks"""
        for parent_task in parent_tasks:
            self.add_parent(parent_task)
