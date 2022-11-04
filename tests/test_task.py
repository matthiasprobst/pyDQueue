import pathlib
import random
import unittest
from typing import Union

import pydqueue as dq


@dq.task
class Simulation:
    """Object used to create instances for each task"""

    def __init__(self, simulation_filename: Union[str, pathlib.Path]):
        self.simulation_filename = pathlib.Path(simulation_filename)

    def task(self, flag, input_data, **kwargs):
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


class TestTask(unittest.TestCase):

    def test_task(self):
        t = Simulation('testfile')
        self.assertIsInstance(t, dq.core.Task)
        t.run('hallo')
