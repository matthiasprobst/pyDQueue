import pathlib
import random
import unittest
from typing import Union

import pydqueue as dq


class TestQueue(unittest.TestCase):

    def test_queue_info(self):

        @dq.task
        def my_func(input_param: float):
            """testing func"""
            print('input: ', input_param)
            return input_param * 2

        self.assertIsInstance(my_func, dq.core.TaskDecorator)
        q = dq.Queue()

        self.assertEqual(q.__str__(), "<Empty Queue>")
        q.info()

        f1 = my_func('one')
        self.assertIsInstance(f1, dq.core.Task)

        f2 = my_func('two')
        self.assertIsInstance(f2, dq.core.Task)

        f3 = my_func('three')
        self.assertIsInstance(f3, dq.core.Task)

        q.append(f1)
        q.append(f2)
        q.append(f3)

        self.assertEqual(q.get_infostr(), 'Task<0>() --> Task<1>() --> Task<2>()')
        q[1].add_parent(q[0])
        q[2].add_parents(q[1], q[0])
        self.assertEqual(q.get_infostr(), 'Task<0>() --> Task<1>(Task<0>) --> Task<2>(Task<1>,Task<0>)')
        self.assertEqual(q.__str__(), 'My_func<0>() --> My_func<1>(My_func<0>) --> My_func<2>(My_func<1>,My_func<0>)')

        q.run(initial=dict(input_param=1), stop_queue_on_error=True, verbose=True)

        q.report()

    def test_using_class(self):

        @dq.task
        class Simulation:
            """Object used to create instances for each task"""

            def __init__(self, simulation_filename: Union[str, pathlib.Path]):
                self.simulation_filename = pathlib.Path(simulation_filename)

            def run(self, input_data, flag=None, **kwargs):
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

        # initialize the first simulation
        A = Simulation('one')
        # initialize the second simulation
        B = Simulation('two')
        C = Simulation('three')
        D = Simulation('four')
        E = Simulation('4')

        q = dq.Queue([A, B, C, D, E])
        self.assertEqual(q.get_infostr(), 'Task<0>() --> Task<1>() --> Task<2>() --> Task<3>() --> Task<4>()')

        # re-building the queue should not influence the result:
        q = dq.Queue([A, B, C, D, E])
        with self.assertRaises(KeyError):
            q[1].add_parent(A)
        q[1].add_parent(q[0])
        q[2].add_parents([q[0], q[1]])
        q[3].add_parent(q[0])
        with self.assertRaises(KeyError):
            q[4].add_parents(D, A)
        q[4].add_parents(q[3], q[0])
        self.assertEqual(q.get_infostr(), 'Task<0>() --> Task<1>(Task<0>) --> Task<2>(Task<0>,Task<1>) --> '
                                          'Task<3>(Task<0>) --> Task<4>(Task<3>,Task<0>)')

        # q[1].add_parent(q[0])
        #
        # q.run(initial=dict(input_data={}), verbose=True)
        # q.report()

    def test_using_function(self):

        def randomfun():
            """method MUST take arguments"""
            rm = random.random()
            if rm < 0.5:
                1 / 0  # trigger an error
                return False, {}  # will not reach this
            return True, {}

        mytask = dq.task(randomfun)

        q = dq.Queue([mytask() for _ in range(10)])
        q.run()
        print(q)
        q.report()

        for task in q.tasks:
            if task.error_message:
                self.assertIsInstance(task.error_message, Exception)

    def test_manipulating_tasks_in_queue(self):
        def dummy(flag, data):
            return 0, {}

        mytask = dq.task(dummy)

        t1 = mytask()
        t2 = mytask()
        q = dq.Queue([t1, t2])
        self.assertEqual(q.__str__(), 'Dummy<0>() --> Dummy<1>()')
        self.assertIsInstance(q[0], dq.core.Task)
        self.assertIsInstance(q[1], dq.core.Task)
        with self.assertRaises(IndexError):
            q[2]
        with self.assertRaises(KeyError):
            q[1].add_parent(t1)
        q[1].add_parent(q[0])
        self.assertEqual(q.__str__(), 'Dummy<0>() --> Dummy<1>(Dummy<0>)')
        q[1].remove_parent(0)
        self.assertEqual(q.__str__(), 'Dummy<0>() --> Dummy<1>()')
