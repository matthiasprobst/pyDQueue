"""Module containing the task class"""
from itertools import count
from typing import Callable, Dict, Tuple, Union

task_id = count()


class Task:
    """Task class"""

    def __init__(self, function: Callable, name: str = None):
        self._id = next(task_id)
        self._name = name
        self.function = function
        self.parents = []
        self.success = None
        self.output = None

    def __call__(self, parent_success: Union[bool, None], input_data: Dict) -> Tuple[bool, Dict]:
        return self.function(parent_success, input_data)

    def __repr__(self) -> str:
        return f'{self.name}'

    @property
    def has_parents(self) -> bool:
        """If number of parents is unequal to zero"""
        return len(self.parents)

    @property
    def name(self) -> str:
        """Return the task name. If self._name is None a name based on the task
        id is generated"""
        if self._name is None:
            return f'Task{self._id}'
        return self._name

    def add_parent(self, parent_task: "Task") -> None:
        """Add a parent task whichs output is taken for input

        Parameters
        ----------
        parent_task: Task
            The task to take as input
        """
        for p in self.parents:
            if parent_task == p:
                raise ValueError(f'Task "{parent_task}" already exists as parent task!')
        if self._id == parent_task._id:
            raise RuntimeError('Cannot add a task to itself!')
        if self._id < parent_task._id and parent_task.has_parents:
            raise RuntimeError('A task added must has no parents or be computed before this task!')
        self.parents.append(parent_task)

    def add_parents(self, *parent_tasks: "Task") -> None:
        """Add multiple parent tasks"""
        for pb in parent_tasks:
            self.add_parent(pb)
