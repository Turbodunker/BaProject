
import io
import importlib
import os
import unittest

from random import shuffle
from shutil import copy
from time import sleep
from warnings import warn

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_monitor import BaseMonitor
from meow_base.conductors import LocalPythonConductor
from meow_base.conductors import LocalBashConductor
from meow_base.core.vars import JOB_TYPE_PAPERMILL, JOB_ERROR, \
    META_FILE, JOB_TYPE_PYTHON, JOB_TYPE_BASH, JOB_CREATE_TIME, get_result_file
from meow_base.core.runner import MeowRunner
from meow_base.functionality.file_io import make_dir, read_file, \
    read_notebook, read_yaml, write_file, lines_to_string
from meow_base.functionality.meow import create_parameter_sweep
from meow_base.functionality.requirements import create_python_requirements
from meow_base.patterns.file_event_pattern import WatchdogMonitor, \
    FileEventPattern
from meow_base.recipes.jupyter_notebook_recipe import PapermillHandler, \
    JupyterNotebookRecipe
from meow_base.recipes.python_recipe import PythonHandler, PythonRecipe
from meow_base.recipes.bash_recipe import BashHandler, BashRecipe
from shared import TEST_JOB_QUEUE, TEST_JOB_OUTPUT, TEST_MONITOR_BASE, \
    MAKER_RECIPE, APPENDING_NOTEBOOK, COMPLETE_PYTHON_SCRIPT, COMPLETE_BASH_SCRIPT, TEST_DIR, \
    FILTER_RECIPE, POROSITY_CHECK_NOTEBOOK, SEGMENT_FOAM_NOTEBOOK, \
    GENERATOR_NOTEBOOK, FOAM_PORE_ANALYSIS_NOTEBOOK, IDMC_UTILS_PYTHON_SCRIPT, \
    TEST_DATA, GENERATE_PYTHON_SCRIPT, setup, backup_before_teardown

pattern_check = FileEventPattern(
    "pattern_check", 
    os.path.join("foam_ct_data", "*"), 
    "recipe_check", 
    "input_filename",
    parameters={
        "output_filedir_accepted": 
            os.path.join("{BASE}", "foam_ct_data_accepted"),
        "output_filedir_discarded": 
            os.path.join("{BASE}", "foam_ct_data_discarded"),
        "porosity_lower_threshold": 0.8,
        "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
    })

pattern_segment = FileEventPattern(
    "pattern_segment",
    os.path.join("foam_ct_data_accepted", "*"),
    "recipe_segment",
    "input_filename",
    parameters={
        "output_filedir": os.path.join("{BASE}", "foam_ct_data_segmented"),
        "input_filedir": os.path.join("{BASE}", "foam_ct_data"),
        "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
    })

pattern_analysis = FileEventPattern(
    "pattern_analysis",
    os.path.join("foam_ct_data_segmented", "*"),
    "recipe_analysis",
    "input_filename",
    parameters={
        "output_filedir": os.path.join("{BASE}", "foam_ct_data_pore_analysis"),
        "utils_path": os.path.join("{BASE}", "idmc_utils_module.py")
    })

pattern_regenerate = FileEventPattern(
    "pattern_regenerate",
    os.path.join("foam_ct_data_discarded", "*"),
    "recipe_generator",
    "discarded",
    parameters={
        "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
        "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
        "gen_path": os.path.join("{BASE}", "generator.py"),
        "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
        "vx": 64,
        "vy": 64,
        "vz": 64,
        "res": 3/64,
        "chance_good": 1,
        "chance_small": 0,
        "chance_big": 0
    })


recipe_check_key, recipe_check_req = create_python_requirements(
    modules=["numpy", "importlib", "matplotlib"])
recipe_check = JupyterNotebookRecipe(
    'recipe_check',
    POROSITY_CHECK_NOTEBOOK, 
    requirements={recipe_check_key: recipe_check_req}
)

recipe_segment_key, recipe_segment_req = create_python_requirements(
    modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
recipe_segment = JupyterNotebookRecipe(
    'recipe_segment',
    SEGMENT_FOAM_NOTEBOOK, 
    requirements={recipe_segment_key: recipe_segment_req}
)

recipe_analysis_key, recipe_analysis_req = create_python_requirements(
    modules=["numpy", "importlib", "matplotlib", "scipy", "skimage"])
recipe_analysis = JupyterNotebookRecipe(
    'recipe_analysis',
    FOAM_PORE_ANALYSIS_NOTEBOOK, 
    requirements={recipe_analysis_key: recipe_analysis_req}
)

recipe_generator_key, recipe_generator_req = create_python_requirements(
    modules=["numpy", "matplotlib", "random"])
recipe_generator = JupyterNotebookRecipe(
    'recipe_generator',
    GENERATOR_NOTEBOOK, 
    requirements={recipe_generator_key: recipe_generator_req}           
)

 
class MeowTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    # def tearDown(self)->None:
    #     super().tearDown()
    #     teardown()

    # Test MeowRunner creation
    def testMeowRunnerSetup(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler()
        handler_two = PapermillHandler()
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor()
        conductor_two = LocalPythonConductor()
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitor_one, handler_one, conductor_one)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), 1)
        self.assertEqual(runner.monitors[0], monitor_one)

        self.assertIsInstance(runner.from_monitors, list)
        self.assertEqual(len(runner.from_monitors), 1)
        runner.monitors[0].to_runner.send("monitor test message")
        message = None
        if runner.from_monitors[0].poll(3):
            message = runner.from_monitors[0].recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "monitor test message")

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)

        self.assertIsInstance(runner.from_handlers, list)
        self.assertEqual(len(runner.from_handlers), 1)
        runner.handlers[0].to_runner.send(
            "handler test message")
        message = None
        if runner.from_handlers[0].poll(3):
            message = runner.from_handlers[0].recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "handler test message")

        self.assertIsInstance(runner.conductors, list)        
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)

        runner = MeowRunner(monitors, handlers, conductors)

        self.assertIsInstance(runner.monitors, list)
        for m in runner.monitors:
            self.assertIsInstance(m, BaseMonitor)
        self.assertEqual(len(runner.monitors), len(monitors))
        self.assertIn(monitor_one, runner.monitors)
        self.assertIn(monitor_two, runner.monitors)

        self.assertIsInstance(runner.from_monitors, list)
        self.assertEqual(len(runner.from_monitors), len(monitors))
        for rm in runner.monitors:
            rm.to_runner.send("monitor test message")
        messages = [None] * len(monitors)
        for i, rfm in enumerate(runner.from_monitors):
            if rfm.poll(3):
                messages[i] = rfm.recv()
        for m in messages:
            self.assertIsNotNone(m)
            self.assertEqual(m, "monitor test message")

        self.assertIsInstance(runner.handlers, list)
        for handler in runner.handlers:
            self.assertIsInstance(handler, BaseHandler)

        self.assertIsInstance(runner.from_handlers, list)
        self.assertEqual(len(runner.from_handlers), len(handlers))
        for rh in runner.handlers:
            rh.to_runner.send("handler test message")
        message = None
        if runner.from_handlers[0].poll(3):
            message = runner.from_handlers[0].recv()
        self.assertIsNotNone(message)
        self.assertEqual(message, "handler test message")

        self.assertIsInstance(runner.conductors, list)
        for conductor in runner.conductors:
            self.assertIsInstance(conductor, BaseConductor)
    
    # Test meow runner directory overrides
    def testMeowRunnerDirOverridesSetup(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {})

        original_queue_dir = os.path.join(TEST_DIR, "original_queue")
        original_output_dir = os.path.join(TEST_DIR, "original_output")
        overridden_queue_dir = os.path.join(TEST_DIR, "overridden_queue")
        overridden_output_dir = os.path.join(TEST_DIR, "overridden_output")

        handler_one = PapermillHandler(job_queue_dir=original_queue_dir)
 
        conductor_one = LocalPythonConductor(
            job_queue_dir=original_queue_dir, 
            job_output_dir=original_output_dir
        )

        self.assertTrue(os.path.exists(original_queue_dir))
        self.assertTrue(os.path.exists(original_output_dir))
        self.assertFalse(os.path.exists(overridden_queue_dir))
        self.assertFalse(os.path.exists(overridden_output_dir))

        self.assertEqual(handler_one.job_queue_dir, original_queue_dir)
        self.assertEqual(conductor_one.job_queue_dir, original_queue_dir)
        self.assertEqual(conductor_one.job_output_dir, original_output_dir)

        MeowRunner(
            monitor_one, 
            handler_one, 
            conductor_one, 
            job_queue_dir=overridden_queue_dir, 
            job_output_dir=overridden_output_dir
        )

        self.assertTrue(os.path.exists(original_queue_dir))
        self.assertTrue(os.path.exists(original_output_dir))
        self.assertTrue(os.path.exists(overridden_queue_dir))
        self.assertTrue(os.path.exists(overridden_output_dir))

        self.assertEqual(handler_one.job_queue_dir, overridden_queue_dir)
        self.assertEqual(conductor_one.job_queue_dir, overridden_queue_dir)
        self.assertEqual(conductor_one.job_output_dir, overridden_output_dir)

    # Test single meow bash job execution
    def testMeowRunnerBashExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one",
            os.path.join("start", "A.txt"), "recipe_one", "infile",
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ),
            BashHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalBashConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3
        )

        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_id = None
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)

                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    loops = 5
            loops += 1

        self.assertIsNotNone(job_id)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        self.assertTrue(os.path.exists(job_dir))
        #Is this necessary when we are we are asserting on the output?
        # result_path = os.path.join(job_dir, get_result_file(JOB_TYPE_BASH))
        # self.assertTrue(os.path.exists(result_path))

        # result = read_file(os.path.join(result_path))
        # self.assertEqual(
        #     result, "--STDOUT--\n12505000\ndone\n\n\n--STDERR--\n\n")

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        output = read_file(os.path.join(output_path))
        self.assertEqual(output, "12505000\n")

    # Test single meow papermill job execution
    def testMeowRunnerPapermillExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_id = None
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    loops = 5
            loops += 1

        self.assertIsNotNone(job_id)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(job_dir)), 5)

        result = read_notebook(
            os.path.join(job_dir, get_result_file(JOB_TYPE_PAPERMILL)))
        self.assertIsNotNone(result)

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        
        with open(output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from a test Pattern")

    # Test meow papermill job chaining within runner
    def testMeowRunnerLinkedPapermillExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from Pattern 1",
                "outfile":os.path.join("{BASE}", "middle", "{FILENAME}")
            })
        pattern_two = FileEventPattern(
            "pattern_two", os.path.join("middle", "A.txt"), "recipe_one", "infile", 
            parameters={
                "extra":"A line from Pattern 2",
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
            pattern_two.name: pattern_two,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE,
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("Initial Data")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while len(job_ids) < 2 and loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    if job_id not in job_ids:
                        job_ids.append(job_id)
            loops += 1

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        self.assertIn(job_ids[0], os.listdir(TEST_JOB_OUTPUT))
        self.assertIn(job_ids[1], os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        mid_job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(mid_job_dir)), 5)

        result = read_notebook(
            os.path.join(mid_job_dir, get_result_file(JOB_TYPE_PAPERMILL)))
        self.assertIsNotNone(result)

        mid_output_path = os.path.join(TEST_MONITOR_BASE, "middle", "A.txt")
        self.assertTrue(os.path.exists(mid_output_path))
        
        with open(mid_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, "Initial Data\nA line from Pattern 1")

        final_job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)
        self.assertEqual(len(os.listdir(final_job_dir)), 5)

        result = read_notebook(os.path.join(final_job_dir, 
            get_result_file(JOB_TYPE_PAPERMILL)))
        self.assertIsNotNone(result)

        final_output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(final_output_path))
        
        with open(final_output_path, "r") as f:
            data = f.read()
        
        self.assertEqual(data, 
            "Initial Data\nA line from Pattern 1\nA line from Pattern 2")

    # Test single meow python job execution
    def testMeowRunnerPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", os.path.join("start", "A.txt"), "recipe_one", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_id = None
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    loops = 5
            loops += 1

        self.assertIsNotNone(job_id)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        result_path = os.path.join(job_dir, get_result_file(JOB_TYPE_PYTHON))
        self.assertTrue(os.path.exists(result_path))
        result = read_file(os.path.join(result_path))
        self.assertEqual(
            result, "--STDOUT--\n12505000.0\ndone\n\n\n--STDERR--\n\n")

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        output = read_file(os.path.join(output_path))
        self.assertEqual(output, "12505000.0")

    # Test meow python job chaining within runner
    def testMeowRunnerLinkedPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "num":250,
                "outfile":os.path.join("{BASE}", "middle", "{FILENAME}")
            })
        pattern_two = FileEventPattern(
            "pattern_two", 
            os.path.join("middle", "A.txt"), 
            "recipe_one", 
            "infile", 
            parameters={
                "num":40,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
            pattern_two.name: pattern_two,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("100")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while len(job_ids) < 2 and loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    if job_id not in job_ids:
                        job_ids.append(job_id)
            loops += 1

        runner.stop()

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        self.assertIn(job_ids[0], os.listdir(TEST_JOB_OUTPUT))
        self.assertIn(job_ids[1], os.listdir(TEST_JOB_OUTPUT))

        meta0 = os.path.join(TEST_JOB_OUTPUT, job_ids[0], META_FILE)
        status0 = read_yaml(meta0)
        create0 = status0[JOB_CREATE_TIME]
        meta1 = os.path.join(TEST_JOB_OUTPUT, job_ids[1], META_FILE)
        status1 = read_yaml(meta1)
        create1 = status1[JOB_CREATE_TIME]
        if create0 < create1:
            mid_job_id = job_ids[0]
            final_job_id = job_ids[1]
        else:
            mid_job_id = job_ids[1]
            final_job_id = job_ids[0]

        mid_job_dir = os.path.join(TEST_JOB_OUTPUT, mid_job_id)
        #Changed this from 5 to 6 because I've added the submit.job
        self.assertEqual(len(os.listdir(mid_job_dir)), 6)

        mid_metafile = os.path.join(mid_job_dir, META_FILE)
        mid_status = read_yaml(mid_metafile)
        self.assertNotIn(JOB_ERROR, mid_status)

        mid_result_path = os.path.join(
            mid_job_dir, get_result_file(JOB_TYPE_PYTHON))
        self.assertTrue(os.path.exists(mid_result_path))
        mid_result = read_file(os.path.join(mid_result_path))
        self.assertEqual(
            mid_result, "--STDOUT--\n7806.25\ndone\n\n\n--STDERR--\n\n")

        mid_output_path = os.path.join(TEST_MONITOR_BASE, "middle", "A.txt")
        self.assertTrue(os.path.exists(mid_output_path))
        mid_output = read_file(os.path.join(mid_output_path))
        self.assertEqual(mid_output, "7806.25")

        final_job_dir = os.path.join(TEST_JOB_OUTPUT, final_job_id)
        #Changed this from 5 to 6 because I've added the submit.job
        self.assertEqual(len(os.listdir(final_job_dir)), 6)

        final_metafile = os.path.join(final_job_dir, META_FILE)
        final_status = read_yaml(final_metafile)
        self.assertNotIn(JOB_ERROR, final_status)

        final_result_path = os.path.join(final_job_dir, get_result_file(JOB_TYPE_PYTHON))
        self.assertTrue(os.path.exists(final_result_path))
        final_result = read_file(os.path.join(final_result_path))
        self.assertEqual(
            final_result, "--STDOUT--\n2146.5625\ndone\n\n\n--STDERR--\n\n")

        final_output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(final_output_path))
        final_output = read_file(os.path.join(final_output_path))
        self.assertEqual(final_output, "2146.5625")

    # Test single meow python job execution
    def testMeowRunnerSweptPythonExecution(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", 
            os.path.join("start", "A.txt"), 
            "recipe_one", 
            "infile",
            sweep=create_parameter_sweep("num", 1000, 10000, 200),
            parameters={
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = []
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_id = job_id[:-2]
                    if job_id not in job_ids:
                        job_ids.append(job_id)
                        loops = 0
            loops += 1

        runner.stop()

        self.assertIsNotNone(job_ids)
        self.assertEqual(len(job_ids), 46)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 46)

        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

            job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

            metafile = os.path.join(job_dir, META_FILE)
            status = read_yaml(metafile)

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(job_dir, get_result_file(JOB_TYPE_PYTHON))
            self.assertTrue(os.path.exists(result_path))
            
            output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
            self.assertTrue(os.path.exists(output_path))

    # Test monitor meow editting
    def testMeowRunnerMEOWEditting(self)->None:
        pattern_one = FileEventPattern(
            "pattern_one", os.path.join("start", "A.txt"), "recipe_one", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        pattern_two = FileEventPattern(
            "pattern_two", os.path.join("start", "A.txt"), "recipe_two", "infile", 
            parameters={
                "num":10000,
                "outfile":os.path.join("{BASE}", "output", "{FILENAME}")
            })
        recipe_one = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT
        )
        recipe_two = PythonRecipe(
            "recipe_two", COMPLETE_PYTHON_SCRIPT
        )

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe_one.name: recipe_one,
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )        
   
        runner.start()

        start_dir = os.path.join(TEST_MONITOR_BASE, "start")
        make_dir(start_dir)
        self.assertTrue(start_dir)
        with open(os.path.join(start_dir, "A.txt"), "w") as f:
            f.write("25000")

        self.assertTrue(os.path.exists(os.path.join(start_dir, "A.txt")))

        loops = 0
        job_ids = set()
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_ids.add(job_id[:-2])
                    loops = 5
            loops += 1

        self.assertEqual(len(job_ids), 1)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.monitors[0].add_pattern(pattern_two)

        loops = 0
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_ids.add(job_id[:-2])
                    loops = 5
            loops += 1

        self.assertEqual(len(job_ids), 1)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 1)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.monitors[0].add_recipe(recipe_two)

        loops = 0
        while loops < 5:
            sleep(1)
            runner_debug_stream.seek(0)
            messages = runner_debug_stream.readlines()

            for msg in messages:
                self.assertNotIn("ERROR", msg)
            
                if "INFO: Completed execution for job: '" in msg:
                    job_id = msg.replace(
                        "INFO: Completed execution for job: '", "")
                    job_ids.add(job_id[:-2])
                    loops = 5
            loops += 1

        self.assertEqual(len(job_ids), 2)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 2)
        for job_id in job_ids:
            self.assertIn(job_id, os.listdir(TEST_JOB_OUTPUT))

        runner.stop()

        job_dir = os.path.join(TEST_JOB_OUTPUT, job_id)

        metafile = os.path.join(job_dir, META_FILE)
        status = read_yaml(metafile)

        self.assertNotIn(JOB_ERROR, status)

        result_path = os.path.join(job_dir, get_result_file(JOB_TYPE_PYTHON))
        self.assertTrue(os.path.exists(result_path))
        result = read_file(os.path.join(result_path))
        self.assertEqual(
            result, "--STDOUT--\n12505000.0\ndone\n\n\n--STDERR--\n\n")

        output_path = os.path.join(TEST_MONITOR_BASE, "output", "A.txt")
        self.assertTrue(os.path.exists(output_path))
        output = read_file(os.path.join(output_path))
        self.assertEqual(output, "12505000.0")

    def testSelfModifyingAnalysis(self)->None:
        maker_pattern = FileEventPattern(
            "maker_pattern", 
            os.path.join("confs", "*.yml"), 
            "maker_recipe", 
            "input_yaml",
            parameters={
                "meow_dir": "self-modifying",
                "filter_recipe": "recipe_filter",
                "recipe_input_image": "input_image",
                "recipe_output_image": "output_image",
                "recipe_args": "args",
                "recipe_method": "method"
            })
        patterns = {
            "maker_pattern": maker_pattern,
        }

        filter_recipe = JupyterNotebookRecipe(
            "filter_recipe", FILTER_RECIPE
        )
        maker_recipe = JupyterNotebookRecipe(
            "maker_recipe", MAKER_RECIPE
        )

        recipes = {
            filter_recipe.name: filter_recipe,
            maker_recipe.name: maker_recipe
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PythonHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )        

        # TODO finish me
#        runner.start()

    # Test some actual scientific analysis, but in a simple progression
    def testScientificAnalysisAllGood(self)->None:
        if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
            warn("Skipping testScientificAnalysisAllGood")
            return
        
        patterns = {
            'pattern_check': pattern_check,
            'pattern_segment': pattern_segment,
            'pattern_analysis': pattern_analysis,
            'pattern_regenerate': pattern_regenerate
        }

        recipes = {
            'recipe_check': recipe_check,
            'recipe_segment': recipe_segment,
            'recipe_analysis': recipe_analysis,
            'recipe_generator': recipe_generator
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )

        good = 3
        big = 0
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)

        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)

        runner.start()

        idle_loops = 0
        total_loops = 0
        messages = None
        while idle_loops < 15 and total_loops < 150:
            sleep(1)
            runner_debug_stream.seek(0)
            new_messages = runner_debug_stream.readlines()

            if messages == new_messages:               
                idle_loops += 1
            else:
                idle_loops = 0
                messages = new_messages
            total_loops += 1

        for message in messages:
            print(message.replace('\n', ''))

        runner.stop()

        print(f"total_loops:{total_loops}, idle_loops:{idle_loops}")

        if len(os.listdir(TEST_JOB_OUTPUT)) != good * 3:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-all_good-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-all_good-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-all_good-{TEST_MONITOR_BASE}")
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), good * 3)
        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-all_good-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-all_good-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-all_good-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, get_result_file(JOB_TYPE_PAPERMILL))
            self.assertTrue(os.path.exists(result_path))

    # Test some actual scientific analysis, in a predicatable loop
    def testScientificAnalysisPredictableLoop(self)->None:
        if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
            warn("Skipping testScientificAnalysisPredictableLoop")
            return
        
        patterns = {
            'pattern_check': pattern_check,
            'pattern_segment': pattern_segment,
            'pattern_analysis': pattern_analysis,
            'pattern_regenerate': pattern_regenerate
        }

        recipes = {
            'recipe_check': recipe_check,
            'recipe_segment': recipe_segment,
            'recipe_analysis': recipe_analysis,
            'recipe_generator': recipe_generator
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )

        good = 10
        big = 5
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        idle_loops = 0
        total_loops = 0
        messages = None
        while idle_loops < 45 and total_loops < 600:
            sleep(1)
            runner_debug_stream.seek(0)
            new_messages = runner_debug_stream.readlines()

            if messages == new_messages:               
                idle_loops += 1
            else:
                idle_loops = 0
                messages = new_messages
            total_loops += 1

        for message in messages:
            print(message.replace('\n', ''))

        runner.stop()
        print(f"total_loops:{total_loops}, idle_loops:{idle_loops}")

        jobs = len(os.listdir(TEST_JOB_OUTPUT))
        if jobs != (good*3 + big*5 + small*5):
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-predictable-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-predictable-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-predictable-{TEST_MONITOR_BASE}")
        
        self.assertEqual(jobs, good*3 + big*5 + small*5)
        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-predictable-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-predictable-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-predictable-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, get_result_file(JOB_TYPE_PAPERMILL))
            self.assertTrue(os.path.exists(result_path))

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))
        if results != good+big+small:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-predictable-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-predictable-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-predictable-{TEST_MONITOR_BASE}")

        self.assertEqual(results, good+big+small)         

    # Test some actual scientific analysis, in an unpredicatable loop
    def testScientificAnalysisRandomLoop(self)->None:
        if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
            warn("Skipping testScientificAnalysisRandomLoop")
            return

        pattern_regenerate_random = FileEventPattern(
            "pattern_regenerate_random",
            os.path.join("foam_ct_data_discarded", "*"),
            "recipe_generator",
            "discarded",
            parameters={
                "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
                "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
                "gen_path": os.path.join("{BASE}", "generator.py"),
                "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
                "vx": 64,
                "vy": 64,
                "vz": 64,
                "res": 3/64,
                "chance_good": 1,
                "chance_small": 0,
                "chance_big": 1
            })

        patterns = {
            'pattern_check': pattern_check,
            'pattern_segment': pattern_segment,
            'pattern_analysis': pattern_analysis,
            'pattern_regenerate_random': pattern_regenerate_random
        }

        recipes = {
            'recipe_check': recipe_check,
            'recipe_segment': recipe_segment,
            'recipe_analysis': recipe_analysis,
            'recipe_generator': recipe_generator
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )

        good = 10
        big = 5
        small = 0
        vx = 64
        vy = 64
        vz = 64
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        idle_loops = 0
        total_loops = 0
        messages = None
        while idle_loops < 60 and total_loops < 600:
            sleep(1)
            runner_debug_stream.seek(0)
            new_messages = runner_debug_stream.readlines()

            if messages == new_messages:               
                idle_loops += 1
            else:
                idle_loops = 0
                messages = new_messages
            total_loops += 1

        for message in messages:
            print(message.replace('\n', ''))

        runner.stop()
        print(f"total_loops:{total_loops}, idle_loops:{idle_loops}")

        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-random-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-random-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-random-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, get_result_file(JOB_TYPE_PAPERMILL))
            self.assertTrue(os.path.exists(result_path))

        outputs = len(os.listdir(TEST_JOB_OUTPUT))
        if outputs < good*3 + big*5 + small*5:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-random-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-random-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-random-{TEST_MONITOR_BASE}")

        self.assertTrue(outputs >= good*3 + big*5 + small*5)

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))

        self.assertEqual(results, good+big+small)

    # Test some actual scientific analysis, in an unpredicatable loop
    def testScientificAnalysisMassiveRandomLoop(self)->None:
        if os.environ["SKIP_LONG"] and os.environ["SKIP_LONG"] == '1':
            warn("Skipping testScientificAnalysisMassiveRandomLoop")
            return

        pattern_regenerate_random = FileEventPattern(
            "pattern_regenerate_random",
            os.path.join("foam_ct_data_discarded", "*"),
            "recipe_generator",
            "discarded",
            parameters={
                "dest_dir": os.path.join("{BASE}", "foam_ct_data"),
                "utils_path": os.path.join("{BASE}", "idmc_utils_module.py"),
                "gen_path": os.path.join("{BASE}", "generator.py"),
                "test_data": os.path.join(TEST_DATA, "foam_ct_data"),
                "vx": 32,
                "vy": 32,
                "vz": 32,
                "res": 3/32,
                "chance_good": 1,
                "chance_small": 0,
                "chance_big": 3
            })

        patterns = {
            'pattern_check': pattern_check,
            'pattern_segment': pattern_segment,
            'pattern_analysis': pattern_analysis,
            'pattern_regenerate_random': pattern_regenerate_random
        }

        recipes = {
            'recipe_check': recipe_check,
            'recipe_segment': recipe_segment,
            'recipe_analysis': recipe_analysis,
            'recipe_generator': recipe_generator
        }

        runner_debug_stream = io.StringIO("")

        runner = MeowRunner(
            WatchdogMonitor(
                TEST_MONITOR_BASE,
                patterns,
                recipes,
                settletime=1
            ), 
            PapermillHandler(
                job_queue_dir=TEST_JOB_QUEUE
            ),
            LocalPythonConductor(),
            job_queue_dir=TEST_JOB_QUEUE,
            job_output_dir=TEST_JOB_OUTPUT,
            print=runner_debug_stream,
            logging=3                
        )

        good = 5
        big = 15
        small = 0
        vx = 32
        vy = 32
        vz = 32
        res = 3/vz
        backup_data_dir = os.path.join(TEST_DATA, "foam_ct_data")
        make_dir(backup_data_dir)
        foam_data_dir = os.path.join(TEST_MONITOR_BASE, "foam_ct_data")
        make_dir(foam_data_dir)
        
        write_file(lines_to_string(IDMC_UTILS_PYTHON_SCRIPT), 
            os.path.join(TEST_MONITOR_BASE, "idmc_utils_module.py"))

        gen_path = os.path.join(TEST_MONITOR_BASE, "generator.py")
        write_file(lines_to_string(GENERATE_PYTHON_SCRIPT), gen_path)

        all_data = [1000] * good + [100] * big + [10000] * small
        shuffle(all_data)

        u_spec = importlib.util.spec_from_file_location("gen", gen_path)
        gen = importlib.util.module_from_spec(u_spec)
        u_spec.loader.exec_module(gen)

        for i, val in enumerate(all_data):
            filename = f"foam_dataset_{i}_{val}_{vx}_{vy}_{vz}.npy"
            backup_file = os.path.join(backup_data_dir, filename)
            if not os.path.exists(backup_file):
                gen.create_foam_data_file(backup_file, val, vx, vy, vz, res)

            target_file = os.path.join(foam_data_dir, filename)
            copy(backup_file, target_file)

        self.assertEqual(len(os.listdir(foam_data_dir)), good + big + small)
        
        runner.start()

        idle_loops = 0
        total_loops = 0
        messages = None
        while idle_loops < 60 and total_loops < 1200:
            sleep(1)
            runner_debug_stream.seek(0)
            new_messages = runner_debug_stream.readlines()

            if messages == new_messages:               
                idle_loops += 1
            else:
                idle_loops = 0
                messages = new_messages
            total_loops += 1

        for message in messages:
            print(message.replace('\n', ''))

        runner.stop()
        print(f"total_loops:{total_loops}, idle_loops:{idle_loops}")

        for job_dir in os.listdir(TEST_JOB_OUTPUT):
            metafile = os.path.join(TEST_JOB_OUTPUT, job_dir, META_FILE)
            status = read_yaml(metafile)

            if JOB_ERROR in status:
                print(status[JOB_ERROR])
                backup_before_teardown(TEST_JOB_OUTPUT, 
                    f"Backup-massive-random-{TEST_JOB_OUTPUT}")
                backup_before_teardown(TEST_JOB_QUEUE, 
                    f"Backup-massive-random-{TEST_JOB_QUEUE}")
                backup_before_teardown(TEST_MONITOR_BASE, 
                    f"Backup-massive-random-{TEST_MONITOR_BASE}")

            self.assertNotIn(JOB_ERROR, status)

            result_path = os.path.join(
                TEST_JOB_OUTPUT, job_dir, get_result_file(JOB_TYPE_PAPERMILL))
            self.assertTrue(os.path.exists(result_path))

        outputs = len(os.listdir(TEST_JOB_OUTPUT))
        if outputs < good*3 + big*5 + small*5:
            backup_before_teardown(TEST_JOB_OUTPUT, 
                f"Backup-massive-random-{TEST_JOB_OUTPUT}")
            backup_before_teardown(TEST_JOB_QUEUE, 
                f"Backup-massive-random-{TEST_JOB_QUEUE}")
            backup_before_teardown(TEST_MONITOR_BASE, 
                f"Backup-massive-random-{TEST_MONITOR_BASE}")
        self.assertTrue(outputs >= good*3 + big*5 + small*5)

        results = len(os.listdir(
            os.path.join(TEST_MONITOR_BASE, "foam_ct_data_pore_analysis")))

        self.assertEqual(results, good+big+small)

    def testMonitorIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        m1 = runner.get_monitor_by_name("m1")
        self.assertEqual(monitor_one, m1)
        m2 = runner.get_monitor_by_name("m2")
        self.assertEqual(monitor_two, m2)
        m3 = runner.get_monitor_by_name("m3")
        self.assertIsNone(m3)

        mt = runner.get_monitor_by_type(WatchdogMonitor)
        self.assertIn(mt, monitors)

    def testHandlerIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        h1 = runner.get_handler_by_name("h1")
        self.assertEqual(handler_one, h1)
        h2 = runner.get_handler_by_name("h2")
        self.assertEqual(handler_two, h2)
        h3 = runner.get_handler_by_name("h3")
        self.assertIsNone(h3)

        mt = runner.get_handler_by_type(PapermillHandler)
        self.assertIn(mt, handlers)
        mn = runner.get_handler_by_type(PythonHandler)
        self.assertIsNone(mn)

    def testConductorIdentification(self)->None:
        monitor_one = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m1")
        monitor_two = WatchdogMonitor(TEST_MONITOR_BASE, {}, {}, name="m2")
        monitors = [ monitor_one, monitor_two ]

        handler_one = PapermillHandler(name="h1")
        handler_two = PapermillHandler(name="h2")
        handlers = [ handler_one, handler_two ]

        conductor_one = LocalPythonConductor(name="c1")
        conductor_two = LocalPythonConductor(name="c2")
        conductors = [ conductor_one, conductor_two ]

        runner = MeowRunner(monitors, handlers, conductors)

        c1 = runner.get_conductor_by_name("c1")
        self.assertEqual(conductor_one, c1)
        c2 = runner.get_conductor_by_name("c2")
        self.assertEqual(conductor_two, c2)
        c3 = runner.get_conductor_by_name("c3")
        self.assertIsNone(c3)

        ct = runner.get_conductor_by_type(LocalPythonConductor)
        self.assertIn(ct, conductors)

    # TODO test getting job cannot handle
    # TODO test getting event cannot handle
    # TODO tests runner job queue dir
    # TODO tests runner job output dir
