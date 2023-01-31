
import json
import unittest
import os

from datetime import datetime
from multiprocessing import Pipe, Queue
from time import sleep

from core.correctness.vars import CHAR_LOWERCASE, CHAR_UPPERCASE, \
    SHA256, TEST_MONITOR_BASE, COMPLETE_NOTEBOOK, EVENT_TYPE, EVENT_PATH, \
    WATCHDOG_TYPE, PYTHON_TYPE, WATCHDOG_BASE, WATCHDOG_HASH, WATCHDOG_RULE, \
    JOB_PARAMETERS, JOB_HASH, PYTHON_FUNC, PYTHON_OUTPUT_DIR, \
    PYTHON_EXECUTION_BASE, APPENDING_NOTEBOOK, JOB_ID, JOB_EVENT, JOB_TYPE, \
    JOB_PATTERN, JOB_RECIPE, JOB_RULE, JOB_STATUS, JOB_CREATE_TIME, \
    JOB_REQUIREMENTS, STATUS_QUEUED
from core.functionality import generate_id, wait, get_file_hash, rmtree, \
    make_dir, parameterize_jupyter_notebook, create_event, create_job, \
    replace_keywords, write_yaml, write_notebook, read_yaml, read_notebook, \
    KEYWORD_PATH, KEYWORD_REL_PATH, KEYWORD_DIR, KEYWORD_REL_DIR, \
    KEYWORD_FILENAME, KEYWORD_PREFIX, KEYWORD_BASE, KEYWORD_EXTENSION, \
    KEYWORD_JOB
from core.meow import create_rule
from patterns import FileEventPattern
from recipes import JupyterNotebookRecipe
from shared import setup, teardown

class CorrectnessTests(unittest.TestCase):
    def setUp(self)->None:
        super().setUp()
        setup()

    def tearDown(self)->None:
        super().tearDown()
        teardown()
        
    def testGenerateIDWorking(self)->None:
        id = generate_id()
        self.assertEqual(len(id), 16)
        for i in range(len(id)):
            self.assertIn(id[i], CHAR_UPPERCASE+CHAR_LOWERCASE)

        # In extrememly rare cases this may fail due to randomness in algorithm
        new_id = generate_id(existing_ids=[id])
        self.assertNotEqual(id, new_id)

        another_id = generate_id(length=32)
        self.assertEqual(len(another_id), 32)

        again_id = generate_id(charset="a")
        for i in range(len(again_id)):
            self.assertIn(again_id[i], "a")

        with self.assertRaises(ValueError):
            generate_id(length=2, charset="a", existing_ids=["aa"])

        prefix_id = generate_id(length=4, prefix="Test")
        self.assertEqual(prefix_id, "Test")

        prefix_id = generate_id(prefix="Test")
        self.assertEqual(len(prefix_id), 16)
        self.assertTrue(prefix_id.startswith("Test"))
    
    def testWaitPipes(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        
        inputs = [
            pipe_one_reader, pipe_two_reader
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            elif readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

    def testWaitQueues(self)->None:
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            queue_one, queue_two
        ]

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

    def testWaitPipesAndQueues(self)->None:
        pipe_one_reader, pipe_one_writer = Pipe()
        pipe_two_reader, pipe_two_writer = Pipe()
        queue_one = Queue()
        queue_two = Queue()

        inputs = [
            pipe_one_reader, pipe_two_reader, queue_one, queue_two
        ]

        pipe_one_writer.send(1)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].recv()
        self.assertEqual(msg, 1)

        pipe_one_writer.send(1)
        pipe_two_writer.send(2)
        readables = wait(inputs)

        self.assertIn(pipe_one_reader, readables)
        self.assertIn(pipe_two_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == pipe_one_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 1)
            if readable == pipe_two_reader:
                msg = readable.recv()        
                self.assertEqual(msg, 2)

        queue_one.put(1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertEqual(len(readables), 1)
        msg = readables[0].get()
        self.assertEqual(msg, 1)

        queue_one.put(1)
        queue_two.put(2)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(queue_two, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == queue_two:
                msg = readable.get()
                self.assertEqual(msg, 2)

        queue_one.put(1)
        pipe_one_writer.send(1)
        sleep(0.1)
        readables = wait(inputs)

        self.assertIn(queue_one, readables)
        self.assertIn(pipe_one_reader, readables)
        self.assertEqual(len(readables), 2)
        for readable in readables:
            if readable == queue_one:
                msg = readable.get()
                self.assertEqual(msg, 1)
            elif readable == pipe_one_reader:
                msg = readable.recv()
                self.assertEqual(msg, 1)

    def testGetFileHashSha256(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "hased_file.txt")
        with open(file_path, 'w') as hashed_file:
            hashed_file.write("Some data\n")
        expected_hash = \
            "8557122088c994ba8aa5540ccbb9a3d2d8ae2887046c2db23d65f40ae63abade"
        
        hash = get_file_hash(file_path, SHA256)
        self.assertEqual(hash, expected_hash)
    
    def testGetFileHashSha256NoFile(self)->None:
        file_path = os.path.join(TEST_MONITOR_BASE, "file.txt")

        with self.assertRaises(FileNotFoundError):        
            get_file_hash(file_path, SHA256)

    def testParameteriseNotebook(self)->None:
        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"a": 4})
        
        self.assertEqual(pn, COMPLETE_NOTEBOOK)

        pn = parameterize_jupyter_notebook(
            COMPLETE_NOTEBOOK, {"s": 4})

        self.assertNotEqual(pn, COMPLETE_NOTEBOOK)
        self.assertEqual(
            pn["cells"][0]["source"], 
            "# The first cell\n\ns = 4\nnum = 1000")

    def testCreateEvent(self)->None:
        event = create_event("test", "path")

        self.assertEqual(type(event), dict)
        self.assertTrue(EVENT_TYPE in event.keys())
        self.assertEqual(len(event.keys()), 2)
        self.assertEqual(event[EVENT_TYPE], "test")
        self.assertEqual(event[EVENT_PATH], "path")

        event2 = create_event("test2", "path2", {"a":1})

        self.assertEqual(type(event2), dict)
        self.assertTrue(EVENT_TYPE in event2.keys())
        self.assertEqual(len(event2.keys()), 3)
        self.assertEqual(event2[EVENT_TYPE], "test2")
        self.assertEqual(event2[EVENT_PATH], "path2")
        self.assertEqual(event2["a"], 1)

    def testCreateJob(self)->None:
        pattern = FileEventPattern(
            "pattern", 
            "file_path", 
            "recipe_one", 
            "infile", 
            parameters={
                "extra":"A line from a test Pattern",
                "outfile":"result_path"
            })
        recipe = JupyterNotebookRecipe(
            "recipe_one", APPENDING_NOTEBOOK)

        rule = create_rule(pattern, recipe)

        event = create_event(
            WATCHDOG_TYPE,
            "file_path",
            {
                WATCHDOG_BASE: TEST_MONITOR_BASE,
                WATCHDOG_RULE: rule,
                WATCHDOG_HASH: "file_hash"
            }
        )

        job_dict = create_job(
            PYTHON_TYPE,
            event,
            {
                JOB_PARAMETERS:{
                    "extra":"extra",
                    "infile":"file_path",
                    "outfile":"result_path"
                },
                JOB_HASH: "file_hash",
                PYTHON_FUNC:max,
                PYTHON_OUTPUT_DIR:"output",
                PYTHON_EXECUTION_BASE:"execution"
            }
        )

        self.assertIsInstance(job_dict, dict)
        self.assertIn(JOB_ID, job_dict)
        self.assertIsInstance(job_dict[JOB_ID], str)
        self.assertIn(JOB_EVENT, job_dict)
        self.assertEqual(job_dict[JOB_EVENT], event)
        self.assertIn(JOB_TYPE, job_dict)
        self.assertEqual(job_dict[JOB_TYPE], PYTHON_TYPE)
        self.assertIn(JOB_PATTERN, job_dict)
        self.assertEqual(job_dict[JOB_PATTERN], pattern)
        self.assertIn(JOB_RECIPE, job_dict)
        self.assertEqual(job_dict[JOB_RECIPE], recipe)
        self.assertIn(JOB_RULE, job_dict)
        self.assertEqual(job_dict[JOB_RULE], rule.name)
        self.assertIn(JOB_STATUS, job_dict)
        self.assertEqual(job_dict[JOB_STATUS], STATUS_QUEUED)
        self.assertIn(JOB_CREATE_TIME, job_dict)
        self.assertIsInstance(job_dict[JOB_CREATE_TIME], datetime)
        self.assertIn(JOB_REQUIREMENTS, job_dict)
        self.assertEqual(job_dict[JOB_REQUIREMENTS], {})

    def testReplaceKeywords(self)->None:
        test_dict = {
            "A": f"--{KEYWORD_PATH}--",
            "B": f"--{KEYWORD_REL_PATH}--",
            "C": f"--{KEYWORD_DIR}--",
            "D": f"--{KEYWORD_REL_DIR}--",
            "E": f"--{KEYWORD_FILENAME}--",
            "F": f"--{KEYWORD_PREFIX}--",
            "G": f"--{KEYWORD_BASE}--",
            "H": f"--{KEYWORD_EXTENSION}--",
            "I": f"--{KEYWORD_JOB}--",
            "J": f"--{KEYWORD_PATH}-{KEYWORD_PATH}--",
            "K": f"{KEYWORD_PATH}",
            "L": f"--{KEYWORD_PATH}-{KEYWORD_REL_PATH}-{KEYWORD_DIR}-"
                 f"{KEYWORD_REL_DIR}-{KEYWORD_FILENAME}-{KEYWORD_PREFIX}-"
                 f"{KEYWORD_BASE}-{KEYWORD_EXTENSION}-{KEYWORD_JOB}--",
            "M": "A",
            "N": 1
        }

        print(test_dict["A"])

        replaced = replace_keywords(
            test_dict, "job_id", "base/src/dir/file.ext", "base/monitor/dir")

        self.assertIsInstance(replaced, dict)
        self.assertEqual(len(test_dict.keys()), len(replaced.keys()))
        for k in test_dict.keys():
            self.assertIn(k, replaced)

        self.assertEqual(replaced["A"], "--base/src/dir/file.ext--")
        self.assertEqual(replaced["B"], "--../../src/dir/file.ext--")
        self.assertEqual(replaced["C"], "--base/src/dir--")
        self.assertEqual(replaced["D"], "--../../src/dir--")
        self.assertEqual(replaced["E"], "--file.ext--")
        self.assertEqual(replaced["F"], "--file--")
        self.assertEqual(replaced["G"], "--base/monitor/dir--")
        self.assertEqual(replaced["H"], "--.ext--")
        self.assertEqual(replaced["I"], "--job_id--")
        self.assertEqual(replaced["J"], 
            "--base/src/dir/file.ext-base/src/dir/file.ext--")
        self.assertEqual(replaced["K"], "base/src/dir/file.ext")
        self.assertEqual(replaced["L"], 
            "--base/src/dir/file.ext-../../src/dir/file.ext-base/src/dir-"
            "../../src/dir-file.ext-file-base/monitor/dir-.ext-job_id--")
        self.assertEqual(replaced["M"], "A") 
        self.assertEqual(replaced["N"], 1) 

    def testWriteNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "test_notebook.ipynb")
        self.assertFalse(os.path.exists(notebook_path))
        write_notebook(APPENDING_NOTEBOOK, notebook_path)
        self.assertTrue(os.path.exists(notebook_path))

        with open(notebook_path, 'r') as f:
            data = f.readlines()
        
        print(data)
        expected_bytes = [
            '{"cells": [{"cell_type": "code", "execution_count": null, '
            '"metadata": {}, "outputs": [], "source": ["# Default parameters '
            'values\\n", "# The line to append\\n", "extra = \'This line '
            'comes from a default pattern\'\\n", "# Data input file '
            'location\\n", "infile = \'start/alpha.txt\'\\n", "# Output file '
            'location\\n", "outfile = \'first/alpha.txt\'"]}, {"cell_type": '
            '"code", "execution_count": null, "metadata": {}, "outputs": [], '
            '"source": ["# load in dataset. This should be a text file\\n", '
            '"with open(infile) as input_file:\\n", "    data = '
            'input_file.read()"]}, {"cell_type": "code", "execution_count": '
            'null, "metadata": {}, "outputs": [], "source": ["# Append the '
            'line\\n", "appended = data + \'\\\\n\' + extra"]}, {"cell_type": '
            '"code", "execution_count": null, "metadata": {}, "outputs": [], '
            '"source": ["import os\\n", "\\n", "# Create output directory if '
            'it doesn\'t exist\\n", "output_dir_path = '
            'os.path.dirname(outfile)\\n", "\\n", "if output_dir_path:\\n", '
            '"    os.makedirs(output_dir_path, exist_ok=True)\\n", "\\n", "# '
            'Save added array as new dataset\\n", "with open(outfile, \'w\') '
            'as output_file:\\n", "   output_file.write(appended)"]}], '
            '"metadata": {"kernelspec": {"display_name": "Python 3", '
            '"language": "python", "name": "python3"}, "language_info": '
            '{"codemirror_mode": {"name": "ipython", "version": 3}, '
            '"file_extension": ".py", "mimetype": "text/x-python", "name": '
            '"python", "nbconvert_exporter": "python", "pygments_lexer": '
            '"ipython3", "version": "3.10.6 (main, Nov 14 2022, 16:10:14) '
            '[GCC 11.3.0]"}, "vscode": {"interpreter": {"hash": '
            '"916dbcbb3f70747c44a77c7bcd40155683ae19c65e1c03b4aa3499c5328201f1'
            '"}}}, "nbformat": 4, "nbformat_minor": 4}'
        ]

        self.assertEqual(data, expected_bytes)

    def testReadNotebook(self)->None:
        notebook_path = os.path.join(TEST_MONITOR_BASE, "test_notebook.ipynb")
        write_notebook(APPENDING_NOTEBOOK, notebook_path)

        notebook = read_notebook(notebook_path)
        self.assertEqual(notebook, APPENDING_NOTEBOOK)


        with self.assertRaises(FileNotFoundError):
            read_notebook("doesNotExist.ipynb")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(ValueError):
            read_notebook(filepath)

        filepath = os.path.join(TEST_MONITOR_BASE, "T.ipynb")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(json.decoder.JSONDecodeError):
            read_notebook(filepath)

    def testWriteYaml(self)->None:
        yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")

        self.assertFalse(os.path.exists(filepath))
        write_yaml(yaml_dict, filepath)
        self.assertTrue(os.path.exists(filepath))

        with open(filepath, 'r') as f:
            data = f.readlines()
        
        expected_bytes = [
            'A: a\n', 
            'B: 1\n',
            'C:\n', 
            '  D: true\n',
            '  E:\n',
            '  - 1\n',
            '  - 2\n',
            '  - 3\n'
        ]
        
        self.assertEqual(data, expected_bytes)

    def testReadYaml(self)->None:
        yaml_dict = {
            "A": "a",
            "B": 1,
            "C": {
                "D": True,
                "E": [
                    1, 2, 3
                ]
            }
        }

        filepath = os.path.join(TEST_MONITOR_BASE, "file.yaml")
        write_yaml(yaml_dict, filepath)

        read_dict = read_yaml(filepath)
        self.assertEqual(yaml_dict, read_dict)

        with self.assertRaises(FileNotFoundError):
            read_yaml("doesNotExist")

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        data = read_yaml(filepath)
        self.assertEqual(data, "Data")

    def testMakeDir(self)->None:
        testDir = os.path.join(TEST_MONITOR_BASE, "Test")
        self.assertFalse(os.path.exists(testDir))
        make_dir(testDir)
        self.assertTrue(os.path.exists(testDir))
        self.assertTrue(os.path.isdir(testDir))

        nested = os.path.join(TEST_MONITOR_BASE, "A", "B", "C", "D")
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        make_dir(nested)
        self.assertTrue(os.path.exists(nested))

        with self.assertRaises(FileExistsError):
            make_dir(nested, can_exist=False)

        filepath = os.path.join(TEST_MONITOR_BASE, "T.txt")
        with open(filepath, "w") as f:
            f.write("Data")

        with self.assertRaises(ValueError):
            make_dir(filepath)

        halfway = os.path.join(TEST_MONITOR_BASE, "A", "B")
        make_dir(halfway, ensure_clean=True)
        self.assertTrue(os.path.exists(halfway))
        self.assertEqual(len(os.listdir(halfway)), 0)

    def testRemoveTree(self)->None:
        nested = os.path.join(TEST_MONITOR_BASE, "A", "B")
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        make_dir(nested)
        self.assertTrue(os.path.exists(nested))

        rmtree(os.path.join(TEST_MONITOR_BASE, "A"))
        self.assertTrue(os.path.exists(TEST_MONITOR_BASE))
        self.assertFalse(os.path.exists(os.path.join(TEST_MONITOR_BASE, "A")))
        self.assertFalse(os.path.exists(
            os.path.join(TEST_MONITOR_BASE, "A", "B")))
