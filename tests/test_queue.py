import pathlib
import random
import unittest
from typing import Dict, Tuple, Union

from pydqueue import Task, Queue


# simulation function that is the same for all tasks
def run_simulation(parents_success: Union[bool, None], input_data: Dict,
                   verbose: bool = False) -> Tuple[bool, Dict]:
    """simulate a simulation. A random variable decides if simulation failes or not"""

    if 'result' in input_data:
        if not parents_success:
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
class Simulation:
    """Object used to create instances for each task"""

    def __init__(self, simulation_filename: pathlib.Path):
        self.simulation_filename = simulation_filename

    def __call__(self, parents_success: Union[bool, None], input_data: Dict,
                 verbose: bool = False):
        return self.run_simulation(parents_success,
                                   input_data,
                                   verbose)

    def run_simulation(self, parents_success: Union[bool, None], input_data: Dict,
                       verbose: bool = False) -> Tuple[bool, Dict]:
        """simulate a simulation. A random variable decides if simulation failes or not"""
        print(f'filename: {self.simulation_filename}')

        if 'result' in input_data:
            if not parents_success:
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
