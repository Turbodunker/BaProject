
import os
import stat
import sys

from typing import Any, Dict, List, Tuple

from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_recipe import BaseRecipe
from meow_base.core.meow import valid_event
from meow_base.functionality.validation import check_type, valid_dict, \
    valid_string, valid_dir_path
from meow_base.core.vars import DEBUG_INFO, DEFAULT_JOB_QUEUE_DIR, \
    VALID_VARIABLE_NAME_CHARS, EVENT_PATH, EVENT_RULE, EVENT_TYPE, JOB_ID, \
    EVENT_TYPE_WATCHDOG, JOB_TYPE_BASH, JOB_PARAMETERS, WATCHDOG_BASE, \
    META_FILE, STATUS_QUEUED, JOB_STATUS, \
    get_base_file, get_job_file
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.file_io import valid_path, make_dir, write_file, \
    lines_to_string, threadsafe_write_status
from meow_base.functionality.parameterisation import parameterize_bash_script
from meow_base.functionality.meow import create_job, replace_keywords


class BashRecipe(BaseRecipe):
    # A path to the bash script used to create this recipe
    def __init__(self, name:str, recipe:Any, parameters:Dict[str,Any]={}, 
            requirements:Dict[str,Any]={}, source:str=""):
        """BashRecipe Constructor. This is used to execute bash scripts, 
        enabling anything not natively supported by MEOW."""
        super().__init__(name, recipe, parameters, requirements)
        self._is_valid_source(source)
        self.source = source

    def _is_valid_source(self, source:str)->None:
        """Validation check for 'source' variable from main constructor."""
        if source:
            valid_path(source, extension=".sh", min_length=0)

    def _is_valid_recipe(self, recipe:List[str])->None:
        """Validation check for 'recipe' variable from main constructor. 
        Called within parent BaseRecipe constructor."""
        check_type(recipe, List, hint="BashRecipe.recipe")
        for line in recipe:
            check_type(line, str, hint="BashRecipe.recipe[line]")

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


class BashHandler(BaseHandler):
    # Config option, above which debug messages are ignored
    debug_level:int
    # Where print messages are sent
    _print_target:Any
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, name:str="",
            print:Any=sys.stdout, logging:int=0, pause_time:int=5)->None:
        """BashHandler Constructor. This creates jobs to be executed as 
        bash scripts. This does not run as a continuous thread to 
        handle execution, but is invoked according to a factory pattern using 
        the handle function. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir will be overwridden by its"""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._print_target, self.debug_level = setup_debugging(print, logging)
        print_debug(self._print_target, self.debug_level, 
            "Created new BashHandler instance", DEBUG_INFO)

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
        Bash recipes"""
        try:
            valid_event(event)
            msg = ""
            if type(event[EVENT_RULE].recipe) != BashRecipe:
                msg = "Recipe is not a BashRecipe. "
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
            JOB_TYPE_BASH, 
            event, 
            extras={
                JOB_PARAMETERS:yaml_dict
            }
        )
        print_debug(self._print_target, self.debug_level,  
            f"Creating job from event at {event[EVENT_PATH]} of type "
            f"{JOB_TYPE_BASH}.", DEBUG_INFO)

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

        # parameterise recipe and write as executeable script
        base_script = parameterize_bash_script(
            event[EVENT_RULE].recipe.recipe, yaml_dict
        )
        base_file = os.path.join(job_dir, get_base_file(JOB_TYPE_BASH))
        write_file(lines_to_string(base_script), base_file)
        os.chmod(base_file, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        # Write job script, to manage base script lifetime and execution

        job_script = assemble_bash_job_script()
        job_file = os.path.join(job_dir, get_job_file(JOB_TYPE_BASH))
        write_file(lines_to_string(job_script), job_file)
        os.chmod(job_file, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        meow_job[JOB_STATUS] = STATUS_QUEUED

        # update the status file with queued status
        threadsafe_write_status(meow_job, meta_file)
        
        # Send job directory, as actual definitons will be read from within it
        self.send_job_to_runner(job_dir)


def assemble_bash_job_script()->List[str]:
    return [
        "#!/bin/bash",
        "",
        "# Get job params",
        "given_hash=$(grep 'file_hash: *' $(dirname $0)/job.yml | tail -n1 | cut -c 14-)",
        "event_path=$(grep 'event_path: *' $(dirname $0)/job.yml | tail -n1 | cut -c 15-)",
        "",
        "echo event_path: $event_path",
        "echo given_hash: $given_hash",
        "",
        "# Check hash of input file to avoid race conditions",
        "actual_hash=$(sha256sum $event_path | cut -c -64)",
        "echo actual_hash: $actual_hash",
        "if [ $given_hash != $actual_hash ]; then",
        "   echo Job was skipped as triggering file has been modified since scheduling",
        "   exit 134",
        "fi",
        "",
        "# Call actual job script",
        "$(dirname $0)/base.sh",
        "",
        "exit $?"
    ]
