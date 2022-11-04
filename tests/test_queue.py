import pathlib
import random
import unittest
from dataclasses import dataclass
from typing import Dict, Tuple, Union

from pydqueue import Task, Queue, TaskCaller


# simulation function that is the same for all tasks
def run_simulation(flag: Union[bool, None], input_data: Dict,
                   verbose: bool = False) -> Tuple[bool, Dict]:
    """simulate a simulation. A random variable decides if simulation failes or not"""

    if 'result' in input_data:
        if not flag:
            input_data['result'] = 0
        if random.random() < 0.5:
            # make simulation fail
            if verbose:
                print('Simulation failed')
            return False, {'result': input_data['result']}
        if verbose:
            print('Simulation succeeded')
        return True, {'result': input_data['result'] + 1}
    if random.random() < 0.5:
        # make simulation fail
        if verbose:
            print('Simulation failed')
        return False, {'result': 0}
    if verbose:
        print('Simulation succeeded')
    return True, {'result': 1}


# if every task needs additional input, this can be solved using a class
# then an instance of that class is passed to each task


@dataclass
class Simulation(TaskCaller):
    """Object used to create instances for each task"""
    simulation_filename: pathlib.Path

    def function(self, flag, input_data, **kwargs):
        """simulate a simulation. A random variable decides if simulation fails or not"""

        if self.simulation_filename is None:
            raise ValueError('Got no simulation filename')

        verbose = kwargs.get('verbose', False)
        if verbose:
            print(f'filename: {self.simulation_filename}')

        if 'result' in input_data:
            if not flag:
                input_data['result'] = 0
            if random.random() < 0.5:
                # make simulation fail
                if verbose:
                    print('Simulation failed')
                return False, {'result': input_data['result']}
            if verbose:
                print('Simulation succeeded')
            return True, {'result': input_data['result'] + 1}
        if random.random() < 0.5:
            # make simulation fail
            if verbose:
                print('Simulation failed')
            return False, {'result': 0}
        if verbose:
            print('Simulation succeeded')
        return True, {'result': 1}


class TestQueue(unittest.TestCase):

    def test_queue(self):
        # A has no parent, because it is the initial task
        A = Task(Simulation('one'), name='Init')

        B = Task(Simulation('two'))
        B.add_parent(A)

        C = Task(Simulation('two'), name='C')
        C.add_parents(B, A)

        D = Task(Simulation('3'), name='D')
        D.add_parents(A)

        E = Task(Simulation('4'), name='E')
        E.add_parents(D, A)

        with self.assertRaises(RuntimeError):
            D.add_parent(D)
        with self.assertRaises(RuntimeError):
            B.add_parent(C)

        q = Queue([A, B, C, D, E])
        print('\n', q)

        q.run({}, verbose=True)
        q.report()

    def test_queue_with_errors(self):
        def random_error(succss, inputdata):
            rm = random.random()
            if rm < 0.5:
                1 / 0  # trigger an error
                return False, {}  # will not reach this
            return True, {}

        q = Queue([Task(random_error) for _ in range(10)])
        q.run({})
        print(q)
        q.report()

    def test_manipuating_taks_in_queue(self):
        def dummy(flag, data):
            return 0, {}

        t1 = Task(dummy)
        t2 = Task(dummy)
        q = Queue([t1, t2])
        self.assertEqual(q.__str__(), 'Task0() --> Task1()')
        self.assertIsInstance(q[0], Task)
        self.assertIsInstance(q[1], Task)
        with self.assertRaises(IndexError):
            q[2]
        q[1].add_parent(t1)
        self.assertEqual(q.__str__(), 'Task0() --> Task1(Task0)')
        q[1].remove_parent(0)
        self.assertEqual(q.__str__(), 'Task0() --> Task1()')
