
"""
This file contains definitions for a MEOW recipe based off of jupyter 
notebooks, along with an appropriate handler for said events.

Author(s): David Marchant
"""
import os
import nbformat
import sys

from typing import Any, Tuple, Dict

from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.base_handler import BaseHandler
from meow_base.core.meow import valid_event
from meow_base.functionality.validation import check_type, valid_string, \
    valid_dict, valid_path, valid_dir_path, valid_existing_file_path
from meow_base.core.vars import VALID_VARIABLE_NAME_CHARS, \
    PYTHON_FUNC, DEBUG_INFO, EVENT_TYPE_WATCHDOG, \
    DEFAULT_JOB_QUEUE_DIR, EVENT_PATH, JOB_TYPE_PAPERMILL, WATCHDOG_HASH, \
    JOB_PARAMETERS, JOB_ID, WATCHDOG_BASE, META_FILE, PARAMS_FILE, \
    JOB_STATUS, STATUS_QUEUED, EVENT_RULE, EVENT_TYPE, EVENT_RULE, \
    get_base_file
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.file_io import make_dir, read_notebook, \
    write_notebook, write_yaml, threadsafe_write_status, \
    threadsafe_update_status
from meow_base.functionality.meow import create_job, replace_keywords


class JupyterNotebookRecipe(BaseRecipe):
    # A path to the jupyter notebook used to create this recipe
    source:str
    def __init__(self, name:str, recipe:Any, parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}, source:str=""):
        """JupyterNotebookRecipe Constructor. This is used to execute analysis 
        code using the papermill module."""
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        """Validation check for 'source' variable from main constructor."""
        if source:
            valid_path(source, extension=".ipynb", min_length=0)

    def _is_valid_recipe(self, recipe:Dict[str,Any])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_type(recipe, Dict, hint="JupyterNotebookRecipe.recipe")
        nbformat.validate(recipe)

    def _is_valid_parameters(self, parameters:Dict[str,Any])->None:
        """Validation check for 'parameters' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(parameters, str, Any, strict=False, min_length=0)
        for k in parameters.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

    def _is_valid_requirements(self, requirements:Dict[str,Any])->None:
        """Validation check for 'requirements' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        valid_dict(requirements, str, Any, strict=False, min_length=0)
        for k in requirements.keys():
            valid_string(k, VALID_VARIABLE_NAME_CHARS)

class PapermillHandler(BaseHandler):
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, name:str="",
            print:Any=sys.stdout, logging:int=0, pause_time:int=5)->None:
        """PapermillHandler Constructor. This creats jobs to be executed using 
        the papermill module. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir will be overwridden."""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new PapermillHandler instance", DEBUG_INFO)

    def handle(self, event:Dict[str,Any])->None:
        """Function called to handle a given event."""
        print_debug(self._print_target, self.debug_level, 
            f"Handling event {event[EVENT_PATH]}", DEBUG_INFO)

        rule = event[EVENT_RULE]

        # Assemble job parameters dict from pattern variables
        yaml_dict = {}
        for var, val in rule.pattern.parameters.items():
            yaml_dict[var] = val
        for var, val in rule.pattern.outputs.items():
            yaml_dict[var] = val
        yaml_dict[rule.pattern.triggering_file] = event[EVENT_PATH]

        # If no parameter sweeps, then one job will suffice
        if not rule.pattern.sweep:
            self.setup_job(event, yaml_dict)
        else:
            # If parameter sweeps, then many jobs created
            values_list = rule.pattern.expand_sweeps()
            for values in values_list:
                for value in values:
                    yaml_dict[value[0]] = value[1]
                self.setup_job(event, yaml_dict)

    def valid_handle_criteria(self, event:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an event defintion, if this handler can 
        process it or not. This handler accepts events from watchdog with 
        jupyter notebook recipes."""
        try:
            valid_event(event)
            msg = ""
            if type(event[EVENT_RULE].recipe) != JupyterNotebookRecipe: 
                msg = "Recipe is not a JupyterNotebookRecipe. "
            if event[EVENT_TYPE] != EVENT_TYPE_WATCHDOG:
                msg += f"Event type is not {EVENT_TYPE_WATCHDOG}."
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)

    def _is_valid_job_queue_dir(self, job_queue_dir)->None:
        """Validation check for 'job_queue_dir' variable from main 
        constructor."""
        valid_dir_path(job_queue_dir, must_exist=False)
        if not os.path.exists(job_queue_dir):
            make_dir(job_queue_dir)

    def setup_job(self, event:Dict[str,Any], yaml_dict:Dict[str,Any])->None:
        """Function to set up new job dict and send it to the runner to be 
        executed."""
        meow_job = create_job(
            JOB_TYPE_PAPERMILL, 
            event, 
            extras={
                JOB_PARAMETERS:yaml_dict,
                PYTHON_FUNC:papermill_job_func,
            }
        )
        print_debug(self._print_target, self.debug_level,  
            f"Creating job from event at {event[EVENT_PATH]} of type "
            f"{JOB_TYPE_PAPERMILL}.", DEBUG_INFO)

        # replace MEOW keyworks within variables dict
        yaml_dict = replace_keywords(
            meow_job[JOB_PARAMETERS],
            meow_job[JOB_ID],
            event[EVENT_PATH],
            event[WATCHDOG_BASE]
        )

        # Create a base job directory
        job_dir = os.path.join(self.job_queue_dir, meow_job[JOB_ID])
        make_dir(job_dir)

        # write a status file to the job directory
        meta_file = os.path.join(job_dir, META_FILE)
        threadsafe_write_status(meow_job, meta_file)

        # write an executable notebook to the job directory
        base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_PAPERMILL))
        write_notebook(event[EVENT_RULE].recipe.recipe, base_file)

        # write a parameter file to the job directory
        param_file = os.path.join(job_dir, PARAMS_FILE)
        write_yaml(yaml_dict, param_file)

        meow_job[JOB_STATUS] = STATUS_QUEUED

        # update the status file with queued status
        threadsafe_update_status(
            {
                JOB_STATUS: STATUS_QUEUED
            },
            meta_file
        )
        
        # Send job directory, as actual definitons will be read from within it
        self.send_job_to_runner(job_dir)

def get_recipe_from_notebook(name:str, notebook_filename:str, 
        parameters:Dict[str,Any]={}, requirements:Dict[str,Any]={}
        )->JupyterNotebookRecipe:
    valid_existing_file_path(notebook_filename, extension=".ipynb")
    check_type(name, str, hint="get_recipe_from_notebook.name")

    notebook_code = read_notebook(notebook_filename)

    return JupyterNotebookRecipe(
        name, 
        notebook_code, 
        parameters=parameters, 
        requirements=requirements, 
        source=notebook_filename
    )

# Papermill job execution code, to be run within the conductor
def papermill_job_func(job_dir):
    # Requires own imports as will be run in its own execution environment
    import os
    import papermill
    from datetime import datetime
    from meow_base.core.vars import JOB_EVENT, JOB_ID, \
        EVENT_PATH, META_FILE, PARAMS_FILE, \
        JOB_STATUS, SHA256, STATUS_SKIPPED, JOB_END_TIME, \
        JOB_ERROR, STATUS_FAILED, get_job_file, \
        get_result_file
    from meow_base.functionality.file_io import read_yaml, write_notebook, \
        threadsafe_read_status, threadsafe_update_status
    from meow_base.functionality.hashing import get_hash
    from meow_base.functionality.parameterisation import parameterize_jupyter_notebook


    # Identify job files
    meta_file = os.path.join(job_dir, META_FILE)
    base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_PAPERMILL))
    job_file = os.path.join(job_dir, get_job_file(JOB_TYPE_PAPERMILL))
    result_file = os.path.join(job_dir, get_result_file(JOB_TYPE_PAPERMILL))
    param_file = os.path.join(job_dir, PARAMS_FILE)

    # Get job defintions   
    job = threadsafe_read_status(meta_file)
    yaml_dict = read_yaml(param_file)

    # Check the hash of the triggering file, if present. This addresses 
    # potential race condition as file could have been modified since 
    # triggering event
    if JOB_EVENT in job and WATCHDOG_HASH in job[JOB_EVENT]:
        # get current hash
        triggerfile_hash = get_hash(job[JOB_EVENT][EVENT_PATH], SHA256)
        # If hash doesn't match, then abort the job. If its been modified, then
        # another job will have been scheduled anyway.
        if not triggerfile_hash \
                or triggerfile_hash != job[JOB_EVENT][WATCHDOG_HASH]:
            msg = "Job was skipped as triggering file " + \
                f"'{job[JOB_EVENT][EVENT_PATH]}' has been modified since " + \
                "scheduling. Was expected to have hash " + \
                f"'{job[JOB_EVENT][WATCHDOG_HASH]}' but has '" + \
                f"{triggerfile_hash}'."
            threadsafe_update_status(
                {
                    JOB_STATUS: STATUS_SKIPPED,
                    JOB_END_TIME: datetime.now(),
                    JOB_ERROR: msg
                },
                meta_file
            )

            return

    # Create a parameterised version of the executable notebook
    try:
        base_notebook = read_notebook(base_file)
        job_notebook = parameterize_jupyter_notebook(
            base_notebook, yaml_dict
        )
        write_notebook(job_notebook, job_file)
    except Exception as e:
        msg = f"Job file {job[JOB_ID]} was not created successfully. {e}"
        threadsafe_update_status(
            {
                JOB_STATUS: STATUS_FAILED,
                JOB_END_TIME: datetime.now(),
                JOB_ERROR: msg
            },
            meta_file
        )
        return

    # Execute the parameterised notebook
    try:
        papermill.execute_notebook(job_file, result_file, {})
    except Exception as e:
        msg = f"Result file {result_file} was not created successfully. {e}"
        threadsafe_update_status(
            {
                JOB_STATUS: STATUS_FAILED,
                JOB_END_TIME: datetime.now(),
                JOB_ERROR: msg
            },
            meta_file
        )

        return
