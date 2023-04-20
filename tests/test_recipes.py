
import jsonschema
import os
import stat
import subprocess
import unittest

from multiprocessing import Pipe
from typing import Dict

from meow_base.core.meow import valid_job
from meow_base.core.vars import EVENT_TYPE, WATCHDOG_BASE, \
    EVENT_RULE, EVENT_TYPE_WATCHDOG, EVENT_PATH, SHA256, WATCHDOG_HASH, \
    JOB_ID, JOB_TYPE_PYTHON, JOB_PARAMETERS, PYTHON_FUNC, \
    JOB_STATUS, META_FILE, JOB_ERROR, PARAMS_FILE, SWEEP_STOP, SWEEP_JUMP, \
    SWEEP_START, JOB_TYPE_PAPERMILL, JOB_TYPE_BASH, \
    get_base_file, get_job_file, get_result_file
from meow_base.core.rule import Rule
from meow_base.functionality.file_io import lines_to_string, make_dir, \
    read_yaml, write_file, write_notebook, write_yaml
from meow_base.functionality.hashing import get_hash
from meow_base.functionality.meow import create_job, create_rules, \
    create_rule, create_watchdog_event
from meow_base.functionality.parameterisation import parameterize_bash_script
from meow_base.patterns.file_event_pattern import FileEventPattern
from meow_base.recipes.bash_recipe import BashRecipe, BashHandler, \
    assemble_bash_job_script
from meow_base.recipes.jupyter_notebook_recipe import JupyterNotebookRecipe, \
    PapermillHandler, papermill_job_func, get_recipe_from_notebook
from meow_base.recipes.python_recipe import PythonRecipe, PythonHandler, \
    python_job_func
from shared import BAREBONES_PYTHON_SCRIPT, COMPLETE_PYTHON_SCRIPT, \
    TEST_JOB_QUEUE, TEST_MONITOR_BASE, TEST_JOB_OUTPUT, BAREBONES_NOTEBOOK, \
    APPENDING_NOTEBOOK, COMPLETE_NOTEBOOK, BAREBONES_BASH_SCRIPT, \
    COMPLETE_BASH_SCRIPT, \
    setup, teardown

class JupyterNotebookTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test JupyterNotebookRecipe can be created
    def testJupyterNotebookRecipeCreationMinimum(self)->None:
        JupyterNotebookRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe can be created with source
    def testJupyterNotebookRecipeCreationSource(self)->None:
        JupyterNotebookRecipe(
            "test_recipe", BAREBONES_NOTEBOOK, source="notebook.ipynb")

    # Test JupyterNotebookRecipe cannot be created without name
    def testJupyterNotebookRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid name
    def testJupyterNotebookRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe("@test_recipe", BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe cannot be created with invalid recipe
    def testJupyterNotebookRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            JupyterNotebookRecipe("test_recipe", {})

    # Test JupyterNotebookRecipe cannot be created with invalid source
    def testJupyterNotebookRecipeCreationInvalidSourceExtension(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="notebook")

    # Test JupyterNotebookRecipe cannot be created with invalid source name
    def testJupyterNotebookRecipeCreationInvalidSoureChar(self)->None:
        with self.assertRaises(ValueError):
            JupyterNotebookRecipe(
                "test_recipe", BAREBONES_NOTEBOOK, source="@notebook.ipynb")

    # Test JupyterNotebookRecipe name setup correctly
    def testJupyterNotebookRecipeSetupName(self)->None:
        name = "name"
        jnr = JupyterNotebookRecipe(name, BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.name, name)

    # Test JupyterNotebookRecipe recipe setup correctly
    def testJupyterNotebookRecipeSetupRecipe(self)->None:
        jnr = JupyterNotebookRecipe("name", BAREBONES_NOTEBOOK)
        self.assertEqual(jnr.recipe, BAREBONES_NOTEBOOK)

    # Test JupyterNotebookRecipe parameters setup correctly
    def testJupyterNotebookRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, parameters=parameters)
        self.assertEqual(jnr.parameters, parameters)

    # Test JupyterNotebookRecipe requirements setup correctly
    def testJupyterNotebookRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, requirements=requirements)
        self.assertEqual(jnr.requirements, requirements)

    # Test JupyterNotebookRecipe source setup correctly
    def testJupyterNotebookRecipeSetupSource(self)->None:
        source = "source.ipynb"
        jnr = JupyterNotebookRecipe(
            "name", BAREBONES_NOTEBOOK, source=source)
        self.assertEqual(jnr.source, source)

class PapermillHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test PapermillHandler can be created
    def testPapermillHanderMinimum(self)->None:
        PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test PapermillHandler naming
    def testPapermillHandlerNaming(self)->None:
        test_name = "test_name"
        handler = PapermillHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = PapermillHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test PapermillHandler will handle given events
    def testPapermillHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test PapermillHandler will create enough jobs from single sweep
    def testPapermillHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test PapermillHandler will create enough jobs from multiple sweeps
    def testPapermillHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    # Test jobFunc performs as expected
    def testJobFunc(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "test")
        result_path = os.path.join(TEST_MONITOR_BASE, "output", "test")

        with open(file_path, "w") as f:
            f.write("Data")

        file_hash = get_hash(file_path, SHA256)

        pattern = FileEventPattern(
            "pattern", 
            file_path, 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":result_path
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        params_dict = {
            "extra":"extra",
            "infile":file_path,
            "outfile":result_path
        }

        job_dict = create_job(
            JOB_TYPE_PAPERMILL,
            create_watchdog_event(
                file_path,
                rule,
                TEST_MONITOR_BASE,
                file_hash
            ),
            extras={
                JOB_PARAMETERS:params_dict,
                PYTHON_FUNC:papermill_job_func
            }
        )

        job_dir = os.path.join(TEST_JOB_QUEUE, job_dict[JOB_ID])
        make_dir(job_dir)

        meta_file = os.path.join(job_dir, META_FILE)
        write_yaml(job_dict, meta_file)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_PAPERMILL))
        write_notebook(APPENDING_NOTEBOOK, base_file)

        papermill_job_func(job_dir)

        job_dir = os.path.join(TEST_JOB_QUEUE, job_dict[JOB_ID])
        self.assertTrue(os.path.exists(job_dir))

        meta_path = os.path.join(job_dir, META_FILE)
        self.assertTrue(os.path.exists(meta_path))
        status = read_yaml(meta_path)
        self.assertIsInstance(status, Dict)
        self.assertIn(JOB_STATUS, status)
        self.assertEqual(status[JOB_STATUS], job_dict[JOB_STATUS])

        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_base_file(JOB_TYPE_PAPERMILL))))
        self.assertTrue(os.path.exists(os.path.join(job_dir, PARAMS_FILE)))
        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_job_file(JOB_TYPE_PAPERMILL))))
        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_result_file(JOB_TYPE_PAPERMILL))))

        self.assertTrue(os.path.exists(result_path))

    # Test jobFunc doesn't execute with no args
    def testJobFuncBadArgs(self)->None:
        try:
            papermill_job_func({})
        except Exception:
            pass

        self.assertEqual(len(os.listdir(TEST_JOB_QUEUE)), 0)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = PapermillHandler()

        pattern = FileEventPattern(
            "pattern_ne", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule"
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule
        })
        self.assertTrue(status)

    # Test function correctly creates Recipe directly from notebook
    def testGetRecipeFromNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "notebook.ipynb")
        write_notebook(COMPLETE_NOTEBOOK, notebook_path)

        self.assertTrue(os.path.exists(notebook_path))

        recipe = get_recipe_from_notebook("name", notebook_path)

        self.assertIsInstance(recipe, JupyterNotebookRecipe)
        self.assertEqual(recipe.name, "name")
        self.assertEqual(recipe.recipe, COMPLETE_NOTEBOOK)

    # Test handler starts and stops appropriatly
    def testPapermillHandlerStartStop(self)->None:
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testPapermillHandlerOngoingHandling(self)->None:
        ph = PapermillHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = JupyterNotebookRecipe(
            "recipe_one", COMPLETE_NOTEBOOK)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)


class PythonTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
 
    # Test PythonRecipe can be created
    def testPythonRecipeCreationMinimum(self)->None:
        PythonRecipe("test_recipe", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created without name
    def testPythonRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            PythonRecipe("", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created with invalid name
    def testPythonRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            PythonRecipe("@test_recipe", BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe cannot be created with invalid recipe
    def testPythonRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(TypeError):
            PythonRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test PythonRecipe name setup correctly
    def testPythonRecipeSetupName(self)->None:
        name = "name"
        pr = PythonRecipe(name, BAREBONES_PYTHON_SCRIPT)
        self.assertEqual(pr.name, name)

    # Test PythonRecipe recipe setup correctly
    def testPythonRecipeSetupRecipe(self)->None:
        pr = PythonRecipe("name", BAREBONES_PYTHON_SCRIPT)
        self.assertEqual(pr.recipe, BAREBONES_PYTHON_SCRIPT)

    # Test PythonRecipe parameters setup correctly
    def testPythonRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        pr = PythonRecipe(
            "name", BAREBONES_PYTHON_SCRIPT, parameters=parameters)
        self.assertEqual(pr.parameters, parameters)

    # Test PythonRecipe requirements setup correctly
    def testPythonRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        pr = PythonRecipe(
            "name", BAREBONES_PYTHON_SCRIPT, requirements=requirements)
        self.assertEqual(pr.requirements, requirements)

class PythonHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test PythonHandler can be created
    def testPythonHandlerMinimum(self)->None:
        PythonHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test PythonHandler naming
    def testPythonHandlerNaming(self)->None:
        test_name = "test_name"
        handler = PythonHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = PythonHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test PythonHandler will handle given events
    def testPythonHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test PythonHandler will create enough jobs from single sweep
    def testPythonHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test PythonHandler will create enough jobs from multiple sweeps
    def testPythonHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    # Test jobFunc performs as expected
    def testJobFunc(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "test")
        result_path = os.path.join(TEST_MONITOR_BASE, "output")

        with open(file_path, "w") as f:
            f.write("250")

        file_hash = get_hash(file_path, SHA256)

        pattern = FileEventPattern(
            "pattern", 
            file_path, 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile": result_path
            })
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        rule = create_rule(pattern, recipe)

        params_dict = {
            "extra":"extra",
            "infile":file_path,
            "outfile": result_path
        }

        job_dict = create_job(
            JOB_TYPE_PYTHON,
            create_watchdog_event(
                file_path,
                rule,
                TEST_MONITOR_BASE,
                file_hash
            ),
            extras={
                JOB_PARAMETERS:params_dict,
                PYTHON_FUNC:python_job_func
            }
        )

        job_dir = os.path.join(TEST_JOB_QUEUE, job_dict[JOB_ID])
        make_dir(job_dir)

        meta_file = os.path.join(job_dir, META_FILE)
        write_yaml(job_dict, meta_file)

        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(params_dict, param_file)

        base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_PYTHON))
        write_notebook(APPENDING_NOTEBOOK, base_file)
        write_file(lines_to_string(COMPLETE_PYTHON_SCRIPT), base_file)

        python_job_func(job_dir)

        self.assertTrue(os.path.exists(job_dir))
        meta_path = os.path.join(job_dir, META_FILE)
        self.assertTrue(os.path.exists(meta_path))

        status = read_yaml(meta_path)
        self.assertIsInstance(status, Dict)
        self.assertIn(JOB_STATUS, status)
        self.assertEqual(status[JOB_STATUS], job_dict[JOB_STATUS])    
        self.assertNotIn(JOB_ERROR, status)

        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_base_file(JOB_TYPE_PYTHON))))
        self.assertTrue(os.path.exists(os.path.join(job_dir, PARAMS_FILE)))
        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_job_file(JOB_TYPE_PYTHON))))
        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_result_file(JOB_TYPE_PYTHON))))

        self.assertTrue(os.path.exists(result_path))

        with open(result_path, "r") as f:
            result = f.read()

        self.assertEqual(result, "124937.5")

    # Test jobFunc doesn't execute with no args
    def testJobFuncBadArgs(self)->None:
        try:
            python_job_func({})
        except Exception:
            pass

        self.assertEqual(len(os.listdir(TEST_JOB_QUEUE)), 0)
        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = PythonHandler()

        pattern = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", BAREBONES_PYTHON_SCRIPT
        )

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule"
        })
        self.assertFalse(status)

        status, s = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule
        })
        self.assertTrue(status)

    # Test handler starts and stops appropriatly
    def testPythonHandlerStartStop(self)->None:
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testPythonHandlerOngoingHandling(self)->None:
        ph = PythonHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = PythonRecipe(
            "recipe_one", COMPLETE_PYTHON_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)


class BashTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
 
    # Test BashRecipe can be created
    def testBashRecipeCreationMinimum(self)->None:
        BashRecipe("test_recipe", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created without name
    def testBashRecipeCreationNoName(self)->None:
        with self.assertRaises(ValueError):
            BashRecipe("", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created with invalid name
    def testBashRecipeCreationInvalidName(self)->None:
        with self.assertRaises(ValueError):
            BashRecipe("@test_recipe", BAREBONES_BASH_SCRIPT)

    # Test BashRecipe cannot be created with invalid recipe
    def testBashRecipeCreationInvalidRecipe(self)->None:
        with self.assertRaises(TypeError):
            BashRecipe("test_recipe", BAREBONES_NOTEBOOK)

    # Test BashRecipe name setup correctly
    def testBashRecipeSetupName(self)->None:
        name = "name"
        pr = BashRecipe(name, BAREBONES_BASH_SCRIPT)
        self.assertEqual(pr.name, name)

    # Test BashRecipe recipe setup correctly
    def testBashRecipeSetupRecipe(self)->None:
        pr = BashRecipe("name", BAREBONES_BASH_SCRIPT)
        self.assertEqual(pr.recipe, BAREBONES_BASH_SCRIPT)

    # Test BashRecipe parameters setup correctly
    def testBashRecipeSetupParameters(self)->None:
        parameters = {
            "a": 1,
            "b": True
        }
        pr = BashRecipe(
            "name", BAREBONES_BASH_SCRIPT, parameters=parameters)
        self.assertEqual(pr.parameters, parameters)

    # Test BashRecipe requirements setup correctly
    def testBashRecipeSetupRequirements(self)->None:
        requirements = {
            "a": 1,
            "b": True
        }
        pr = BashRecipe(
            "name", BAREBONES_BASH_SCRIPT, requirements=requirements)
        self.assertEqual(pr.requirements, requirements)

class BashHandlerTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()

    # Test BashHandler can be created
    def testBashHandlerMinimum(self)->None:
        BashHandler(job_queue_dir=TEST_JOB_QUEUE)

    # Test BashHandler naming
    def testBashHandlerNaming(self)->None:
        test_name = "test_name"
        handler = BashHandler(name=test_name)
        self.assertEqual(handler.name, test_name)

        handler = BashHandler()
        self.assertTrue(handler.name.startswith("handler_"))

    # Test BashHandler will handle given events
    def testBashHandlerHandling(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        if from_handler_to_job_reader.poll(3):
            job_dir = from_handler_to_job_reader.recv()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)

    # Test BashHandler will create enough jobs from single sweep
    def testBashHandlerHandlingSingleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={"s":{
                SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
            }})
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [0, 1, 2]
        self.assertEqual(len(jobs), 3)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            self.assertIn("s", job[JOB_PARAMETERS])
            if job[JOB_PARAMETERS]["s"] in values:
                values.remove(job[JOB_PARAMETERS]["s"])
        self.assertEqual(len(values), 0)

    # Test BashHandler will create enough jobs from multiple sweeps
    def testBashHandlerHandlingMultipleSweep(self)->None:
        from_handler_to_job_reader, from_handler_to_job_writer = Pipe()
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        ph.to_runner_job = from_handler_to_job_writer
        
        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one", sweep={
                "s1":{
                    SWEEP_START: 0, SWEEP_STOP: 2, SWEEP_JUMP:1
                },
                "s2":{
                    SWEEP_START: 20, SWEEP_STOP: 80, SWEEP_JUMP:15
                }
            })
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        self.assertEqual(len(os.listdir(TEST_JOB_OUTPUT)), 0)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        ph.handle(event)

        jobs = []
        recieving = True
        while recieving:
            if from_handler_to_job_reader.poll(3):
                jobs.append(from_handler_to_job_reader.recv())
            else:
                recieving = False

        values = [
            "s1-0/s2-20", "s1-1/s2-20", "s1-2/s2-20", 
            "s1-0/s2-35", "s1-1/s2-35", "s1-2/s2-35", 
            "s1-0/s2-50", "s1-1/s2-50", "s1-2/s2-50", 
            "s1-0/s2-65", "s1-1/s2-65", "s1-2/s2-65", 
            "s1-0/s2-80", "s1-1/s2-80", "s1-2/s2-80", 
        ]
        self.assertEqual(len(jobs), 15)
        for job_dir in jobs:
            self.assertIsInstance(job_dir, str)
            self.assertTrue(os.path.exists(job_dir))

            job = read_yaml(os.path.join(job_dir, META_FILE))
            valid_job(job)

            self.assertIn(JOB_PARAMETERS, job)
            val1 = None
            val2 = None
            if "s1" in job[JOB_PARAMETERS]:
                val1 = f"s1-{job[JOB_PARAMETERS]['s1']}"
            if "s2" in job[JOB_PARAMETERS]:
                val2 = f"s2-{job[JOB_PARAMETERS]['s2']}"
            val = None
            if val1 and val2:
                val = f"{val1}/{val2}"
            if val and val in values:
                values.remove(val)
        self.assertEqual(len(values), 0)

    # Test jobFunc performs as expected
    def testJobFunc(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "test")
        result_path = os.path.join(TEST_MONITOR_BASE, "output")

        with open(file_path, "w") as f:
            f.write("250")

        file_hash = get_hash(file_path, SHA256)

        pattern = FileEventPattern(
            "pattern", 
            file_path, 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile": result_path
            })
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        rule = create_rule(pattern, recipe)

        params_dict = {
            "extra":"extra",
            "infile":file_path,
            "outfile": result_path
        }

        job_dict = create_job(
            JOB_TYPE_BASH,
            create_watchdog_event(
                file_path,
                rule,
                TEST_MONITOR_BASE,
                file_hash
            ),
            extras={
                JOB_PARAMETERS:params_dict
            }
        )

        job_dir = os.path.join(TEST_JOB_QUEUE, job_dict[JOB_ID])
        make_dir(job_dir)

        meta_file = os.path.join(job_dir, META_FILE)
        write_yaml(job_dict, meta_file)

        base_script = parameterize_bash_script(
            COMPLETE_BASH_SCRIPT, params_dict
        )
        base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_BASH))
        write_file(lines_to_string(base_script), base_file)
        st = os.stat(base_file)
        os.chmod(base_file, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        job_script = assemble_bash_job_script()
        job_file = os.path.join(job_dir, get_job_file(JOB_TYPE_BASH))
        write_file(lines_to_string(job_script), job_file)
        st = os.stat(job_file)
        os.chmod(job_file, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        print(os.listdir(job_dir))
        print(os.getcwd())

        result = subprocess.call(job_file, cwd=".")

        self.assertEqual(result, 0)

        self.assertTrue(os.path.exists(job_dir))
        meta_path = os.path.join(job_dir, META_FILE)
        self.assertTrue(os.path.exists(meta_path))

        status = read_yaml(meta_path)
        self.assertIsInstance(status, Dict)
        self.assertIn(JOB_STATUS, status)
        self.assertEqual(status[JOB_STATUS], job_dict[JOB_STATUS])    
        self.assertNotIn(JOB_ERROR, status)

        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_base_file(JOB_TYPE_BASH))))
        self.assertTrue(os.path.exists(
            os.path.join(job_dir, get_job_file(JOB_TYPE_BASH))))

        self.assertTrue(os.path.exists(result_path))

        with open(result_path, "r") as f:
            result = f.read()

        self.assertEqual(result, "124937\n")

    # Test handling criteria function
    def testValidHandleCriteria(self)->None:
        ph = BashHandler()

        pattern = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", BAREBONES_BASH_SCRIPT
        )

        rule = create_rule(pattern, recipe)

        status, _ = ph.valid_handle_criteria({})
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria("")
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: "type",
            EVENT_RULE: rule
        })
        self.assertFalse(status)

        status, _ = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: "rule"
        })
        self.assertFalse(status)

        status, s = ph.valid_handle_criteria({
            EVENT_PATH: "path",
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_RULE: rule
        })
        self.assertTrue(status)

    # Test handler starts and stops appropriatly
    def testBashHandlerStartStop(self)->None:
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        from_handler_to_event_reader, from_handler_to_event_writer = Pipe()
        ph.to_runner_event = from_handler_to_event_writer

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if from_handler_to_event_reader.poll(3):
            msg = from_handler_to_event_reader.recv()

        self.assertTrue(ph._handle_thread.is_alive())
        self.assertEqual(msg, 1)

        ph.stop()

        self.assertFalse(ph._handle_thread.is_alive())

    # Test handler handles given events
    def testBashHandlerOngoingHandling(self)->None:
        ph = BashHandler(job_queue_dir=TEST_JOB_QUEUE)
        handler_to_event_us, handler_to_event_them = Pipe(duplex=True)
        handler_to_job_us, handler_to_job_them = Pipe()
        ph.to_runner_event = handler_to_event_them
        ph.to_runner_job = handler_to_job_them

        with open(os.path.join(TEST_MONITOR_BASE, "A"), "w") as f:
            f.write("Data")

        pattern_one = FileEventPattern(
            "pattern_one", "A", "recipe_one", "file_one")
        recipe = BashRecipe(
            "recipe_one", COMPLETE_BASH_SCRIPT)

        patterns = {
            pattern_one.name: pattern_one,
        }
        recipes = {
            recipe.name: recipe,
        }

        rules = create_rules(patterns, recipes)
        self.assertEqual(len(rules), 1)
        _, rule = rules.popitem()
        self.assertIsInstance(rule, Rule)

        event = {
            EVENT_TYPE: EVENT_TYPE_WATCHDOG,
            EVENT_PATH: os.path.join(TEST_MONITOR_BASE, "A"),
            WATCHDOG_BASE: TEST_MONITOR_BASE,
            EVENT_RULE: rule,
            WATCHDOG_HASH: get_hash(
                os.path.join(TEST_MONITOR_BASE, "A"), SHA256
            )
        }

        with self.assertRaises(AttributeError):
            self.assertFalse(ph._handle_thread.is_alive())

        ph.start()
        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

            handler_to_event_us.send(event)

        if handler_to_job_us.poll(3):
            job_dir = handler_to_job_us.recv()

        if handler_to_event_us.poll(3):
            msg = handler_to_event_us.recv()
            self.assertEqual(msg, 1)

        ph.stop()

        self.assertIsInstance(job_dir, str)
        self.assertTrue(os.path.exists(job_dir))

        job = read_yaml(os.path.join(job_dir, META_FILE))
        valid_job(job)
