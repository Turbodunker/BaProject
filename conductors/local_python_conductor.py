
"""
This file contains definitions for the LocalPythonConductor, in order to 
execute Python jobs on the local resource.

Author(s): David Marchant
"""
import os
import shutil
import subprocess
import yaml
import time
import stat
import glob

from datetime import datetime
from typing import Any, Tuple, Dict, List



from meow_base.core.base_conductor import BaseConductor, assemble_startcontainer_script, assemble_conn_script
from meow_base.core.meow import valid_job
from meow_base.core.vars import DEFAULT_JOB_QUEUE_DIR, \
    DEFAULT_JOB_OUTPUT_DIR, JOB_TYPE, JOB_TYPE_BASH, JOB_TYPE_SLURM, JOB_TYPE_PYTHON, META_FILE, JOB_STATUS, \
    BACKUP_JOB_ERROR_FILE, STATUS_DONE, JOB_END_TIME, STATUS_FAILED, PYTHON_FUNC, JOB_TYPE_PAPERMILL, \
    JOB_ERROR, JOB_TYPE, STATUS_RUNNING, \
    JOB_START_TIME, DEFAULT_JOB_OUTPUT_DIR
from meow_base.functionality.validation import valid_dir_path
from meow_base.functionality.file_io import make_dir, write_file, \
    threadsafe_read_status, threadsafe_update_status, lines_to_string, read_yaml
# from meow_base.core.base_conductor import BaseConductor
# from meow_base.core.meow import valid_job
# from meow_base.core.vars import JOB_TYPE_PYTHON, PYTHON_FUNC, \
#     JOB_STATUS, STATUS_RUNNING, JOB_START_TIME, META_FILE, \
#     BACKUP_JOB_ERROR_FILE, STATUS_DONE, JOB_END_TIME, STATUS_FAILED, \
#     JOB_ERROR, JOB_TYPE, JOB_TYPE_PAPERMILL, DEFAULT_JOB_QUEUE_DIR, \
#     DEFAULT_JOB_OUTPUT_DIR
# from meow_base.functionality.validation import valid_dir_path
# from meow_base.functionality.file_io import make_dir, write_file, \
#     threadsafe_read_status, threadsafe_update_status

class LocalPythonConductor(BaseConductor):
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, 
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR, name:str="", 
                 pause_time:int=5, remote:bool=False, slurmArgs:List[str]=None)->None:
        """LocalPythonConductor Constructor. This should be used to execute 
        Python jobs, and will then pass any internal job runner files to the 
        output directory. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir and job_output_dir will be overwridden."""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._is_valid_job_output_dir(job_output_dir)
        self.job_output_dir = job_output_dir
        self.remote = remote
        self.slurmArgs = slurmArgs

    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an job defintion, if this conductor can 
        process it or not. This conductor will accept any Python job type"""
        try:
            valid_job(job)
            msg = ""
            if job[JOB_TYPE] not in [JOB_TYPE_PYTHON, JOB_TYPE_PAPERMILL]:
                msg = "Job type was not in python or papermill. "
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

    def _is_valid_job_output_dir(self, job_output_dir)->None:
        """Validation check for 'job_output_dir' variable from main 
        constructor."""
        valid_dir_path(job_output_dir, must_exist=False)
        if not os.path.exists(job_output_dir):
            make_dir(job_output_dir)

    # def execute(self, job_dir:str)->None:
        # if self.remote:
        #     valid_dir_path(job_dir, must_exist=True)

        #     try:
        #         meta_file = os.path.join(job_dir, META_FILE)
        #         job = threadsafe_read_status(meta_file)
        #         job_id = job["id"]

        #         # set the correct command for remote
        #         threadsafe_update_status(
        #             {

        #                 "tmp script command": "connect.sh",
        #             },
        #             meta_file
        #         )

        #     except Exception as e:
        #         # If something has gone wrong at this stage then its bad, so we
        #         # need to make our own error file
        #         error_file = os.path.join(job_dir, BACKUP_JOB_ERROR_FILE)
        #         write_file(f"Recieved incorrectly setup job.\n\n{e}", error_file)
        #         abort = True

        #     # Create startcontainer script with job id environment variable
        #     start_script = assemble_startcontainer_script(job_id, job_dir, self.slurmArgs)
        #     path_to_start = os.path.join(job_dir, "startcontainer.sh")
        #     write_file(lines_to_string(start_script), path_to_start)
        #     os.chmod(path_to_start, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IWOTH | stat.S_IWRITE | stat.S_IWUSR)
        #     # os.chmod(path_to_start, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


        #     # Create connect.sh
        #     conn_script = assemble_conn_script(job_dir, self.job_output_dir, self.slurmArgs)
        #     path_to_conn = os.path.join(job_dir, "connect.sh")

        #     write_file(lines_to_string(conn_script), path_to_conn)
        #     os.chmod(path_to_conn, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        #     self.run_job(job_dir)
        # else:
        #     self.run_job(job_dir)
