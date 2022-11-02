import random
import unittest
from typing import Dict, Tuple, Union

from pydqueue import Task, Queue


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


class TestQueue(unittest.TestCase):

    def test_queue(self):
        # A has no parent, because it is the initial task
        A = Task(run_simulation, name='Init')

        B = Task(run_simulation)
        B.add_parent(A)

        C = Task(run_simulation, name='C')
        C.add_parents(B, A)

        D = Task(run_simulation, name='D')
        D.add_parents(A)

        E = Task(run_simulation, name='E')
        E.add_parents(D, A)

        with self.assertRaises(RuntimeError):
            D.add_parent(D)
        with self.assertRaises(RuntimeError):
            B.add_parent(C)

        q = Queue([A, B, C, D, E])
        print('\n', q)

        q.run({}, verbose=True)
