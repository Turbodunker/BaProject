
"""
This file contains definitions for the LocalPythonConductor, in order to 
execute Python jobs on the local resource.

Author(s): David Marchant
"""
import os
import shutil

from datetime import datetime
from typing import Any, Tuple, Dict

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.meow import valid_job
from meow_base.core.vars import JOB_TYPE_PYTHON, PYTHON_FUNC, \
    JOB_STATUS, STATUS_RUNNING, JOB_START_TIME, META_FILE, \
    BACKUP_JOB_ERROR_FILE, STATUS_DONE, JOB_END_TIME, STATUS_FAILED, \
    JOB_ERROR, JOB_TYPE, JOB_TYPE_PAPERMILL, DEFAULT_JOB_QUEUE_DIR, \
    DEFAULT_JOB_OUTPUT_DIR
from meow_base.functionality.validation import valid_dir_path
from meow_base.functionality.file_io import make_dir, write_file, \
    threadsafe_read_status, threadsafe_update_status

class LocalPythonConductor(BaseConductor):
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, 
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR, name:str="", 
            pause_time:int=5)->None:
        """LocalPythonConductor Constructor. This should be used to execute 
        Python jobs, and will then pass any internal job runner files to the 
        output directory. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir and job_output_dir will be overwridden."""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._is_valid_job_output_dir(job_output_dir)
        self.job_output_dir = job_output_dir

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

    def translate_job(self, job_dir:str, job_type:str)->None:
        """Function for a given conductor to translate a given job."""

        job_id = os.path.basename(job_dir)
        if(os.path.exists(f"test_job_output/{job_id}/submit.job")):
            os.remove(f"test_job_output/{job_id}/submit.job")
        #Write the .job file for slurm. Should be scheduled with sbatch in the "home"-directory when mounted.
        with open(f"test_job_output/{job_id}/submit.job", "w+") as fp:
            fp.write("#!/bin/bash\n")
            fp.write("#SBATCH --job-name=submit_{job_id}.job\n")
            fp.write("#SBATCH --output=slurmA.txt\n")
            fp.write("OPATH=test_monitor_base/output;\n")
            fp.write("cd base\n")
            #Maybe split this to a seperate function if it grows much more..
            match job_type:
                case "papermill":
                    fp.write(f"papermill test_job_output/{job_id}/job.ipynb > $OPATH/slurmA.txt\n")
                case "python":
                    fp.write(f"python3 test_job_output/{job_id}/job.py > $OPATH/slurmA.txt\n")
                case "bash":
                    fp.write(f"exec test_job_output/{job_id}/job.sh > $OPATH/slurmA.txt\n")
            #TODO: Make sure the job is finished before unmounting
            fp.write("umount -l $PWD")

    def execute(self, job_dir:str)->None:
        """Function to actually execute a Python job. This will read job 
        defintions from its meta file, update the meta file and attempt to 
        execute. Some unspecific feedback will be given on execution failure, 
        but depending on what it is it may be up to the job itself to provide 
        more detailed feedback."""
        valid_dir_path(job_dir, must_exist=True)

        # Test our job parameters. Even if its gibberish, we still move to 
        # output
        abort = False
        try:
            meta_file = os.path.join(job_dir, META_FILE)
            job = threadsafe_read_status(meta_file)
            valid_job(job)

            # update the status file with running status
            threadsafe_update_status(
                {
                    JOB_STATUS: STATUS_RUNNING,
                    JOB_START_TIME: datetime.now()
                }, 
                meta_file
            )

        except Exception as e:
            # If something has gone wrong at this stage then its bad, so we 
            # need to make our own error file
            error_file = os.path.join(job_dir, BACKUP_JOB_ERROR_FILE)
            write_file(f"Recieved incorrectly setup job.\n\n{e}", error_file)
            abort = True

        # execute the job
        if not abort:
            try:
                job_function = job[PYTHON_FUNC]
                job_function(job_dir)

                # Update the status file with the finalised status
                threadsafe_update_status(
                    {
                        JOB_STATUS: STATUS_DONE,
                        JOB_END_TIME: datetime.now()
                    }, 
                    meta_file
                )

            except Exception as e:
                # Update the status file with the error status. Don't overwrite
                # any more specific error messages already created
                threadsafe_update_status(
                    {
                        JOB_STATUS: STATUS_FAILED,
                        JOB_END_TIME: datetime.now(),
                        JOB_ERROR: f"Job execution failed. {e}"
                    }, 
                    meta_file
                )


        # Move the contents of the execution directory to the final output 
        # directory. 
        job_output_dir = \
            os.path.join(self.job_output_dir, os.path.basename(job_dir))
        shutil.move(job_dir, job_output_dir)

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
