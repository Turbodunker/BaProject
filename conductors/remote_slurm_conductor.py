
"""
This file is based on the LocalBashConductor.

Author(s): David Marchant, Mark Blomqvist
"""

import os
import subprocess
import time
import stat
import glob

from datetime import datetime
from typing import Any, Dict, Tuple, List

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.meow import valid_job
from meow_base.core.vars import DEFAULT_JOB_QUEUE_DIR, \
    DEFAULT_JOB_OUTPUT_DIR, JOB_TYPE, JOB_TYPE_BASH, JOB_TYPE_SLURM, JOB_TYPE_PYTHON, META_FILE, JOB_STATUS, \
    BACKUP_JOB_ERROR_FILE, STATUS_DONE, JOB_END_TIME, STATUS_FAILED, \
    JOB_ERROR, JOB_TYPE, DEFAULT_JOB_QUEUE_DIR, STATUS_RUNNING, \
    JOB_START_TIME, DEFAULT_JOB_OUTPUT_DIR
from meow_base.functionality.validation import valid_dir_path
from meow_base.functionality.file_io import make_dir, write_file, \
    threadsafe_read_status, threadsafe_update_status, lines_to_string, read_yaml


class RemoteSlurmConductor(BaseConductor):
    def __init__(self, slurmArgs:List[str]=None, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR,
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR, name:str="", 
                 pause_time:int=5)->None:
        """RemoteSlurmConductor Constructor. This should be used to create and transmit
        Slurm jobs, and will then pass any internal job runner files to the
        output directory. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir and job_output_dir will be overwridden."""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._is_valid_job_output_dir(job_output_dir)
        self.job_output_dir = job_output_dir
        self.slurmArgs = slurmArgs

    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an job defintion, if this conductor can 
        process it or not. This conductor will accept any Bash job type"""
        try:
            valid_job(job)
            msg = ""
            if job[JOB_TYPE] not in [JOB_TYPE_BASH, JOB_TYPE_PYTHON]:
                msg = f"Job type was not {JOB_TYPE_SLURM}."
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

    def execute(self, job_dir:str)->None:

        valid_dir_path(job_dir, must_exist=True)


        try:
            meta_file = os.path.join(job_dir, META_FILE)
            job = threadsafe_read_status(meta_file)
            job_id = job["id"]

            # set the correct command for remote
            threadsafe_update_status(
                {

                    "tmp script command": "connect.sh",
                },
                meta_file
            )

        except Exception as e:
            # If something has gone wrong at this stage then its bad, so we
            # need to make our own error file
            error_file = os.path.join(job_dir, BACKUP_JOB_ERROR_FILE)
            write_file(f"Recieved incorrectly setup job.\n\n{e}", error_file)
            abort = True

        # Create startcontainer script with job id environment variable
        start_script = assemble_startcontainer_script(job_id, job_dir, self.slurmArgs)
        path_to_start = os.path.join(job_dir, "startcontainer.sh")
        write_file(lines_to_string(start_script), path_to_start)
        os.chmod(path_to_start, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IWOTH | stat.S_IWRITE | stat.S_IWUSR)
        # os.chmod(path_to_start, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


        # Create connect.sh
        conn_script = assemble_conn_script(job_dir, self.job_output_dir, self.slurmArgs)
        path_to_conn = os.path.join(job_dir, "connect.sh")

        write_file(lines_to_string(conn_script), path_to_conn)
        os.chmod(path_to_conn, stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        self.run_job(job_dir)



def assemble_startcontainer_script(job_id:str, job_dir:str, slurmArgs:List[str]=None)->List[str]:
    base = []
    execmethod = ""
    if slurmArgs is None:
        execmethod = f"docker run --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        base = [ execmethod ]
    #srun case
    elif slurmArgs[0] == "srun":
        if len(slurmArgs) == 1:
            execmethod = f"srun docker run --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        elif len(slurmArgs) == 2:
            execmethod = f"srun {slurmArgs[1]} docker run --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        else:
            print("srun arguments should be written as one line as the second entry in slurmArgs!")
            os.exit()
        base = [ execmethod ]
    #sbatch case
    elif slurmArgs[0] == "sbatch":
        execmethod = f"docker run --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        base = [
            f"{arg}\n" for arg in slurmArgs[1:]
            ]
        base = base + [ execmethod ]
    #scrun case
    elif slurmArgs[0] == "scrun":
        execmethod = f"docker run $DOCKER_SECURITY --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        base = [ execmethod ]
    else:
        execmethod = f"docker run --cap-add SYS_ADMIN --device /dev/fuse -e ID={job_id} slurm-cluster"
        base = [ execmethod ]

    #Could make this an argument aswell with some restrictions
    shell = ["#!/bin/env bash"] + base
    # base = [
    #     execmethod
    #     #"shred -u $0"
    # ]
    return shell

def assemble_conn_script(job_dir:str, job_output_dir:str, slurmArgs:List[str]=None)->List[str]:

    # send = ""
    # if slurmArgs != None:
    #     send = f"scp -i ~/.ssh/slurm.key {job_dir}/startcontainer.sh shodan@192.168.0.24:~/cluster"
    execmethod = ""
    if slurmArgs is None:
        execmethod = f"ssh -i ~/.ssh/slurm.key shodan@192.168.0.24 'cd cluster && bash -s' < {job_dir}/startcontainer.sh"
    elif slurmArgs[0] == "sbatch":
        execmethod = f"ssh -i ~/.ssh/slurm.key shodan@192.168.0.24 'cd cluster && sbatch' < {job_dir}/startcontainer.sh"
    else:
        execmethod = f"ssh -i ~/.ssh/slurm.key shodan@192.168.0.24 'cd cluster && bash -s' < {job_dir}/startcontainer.sh"
    #Could make this an argument aswell with some restrictions
    shell = ["#!/bin/bash"]
    base = [
        # f"scp -i ~/.ssh/slurm.key {job_dir}/startcontainer.sh shodan@192.168.0.24:~/cluster",
        "timeout=0",
        execmethod,
        "res=$?",
        "if [ $res -ne 0 ]; then",
        "\twhile [ $timeout -ne 30 ]; do",
        "\t\techo in while loop",
        "\t\t((timeout=timeout+1))",
        "\t\tsleep 1",
        f"\t\t"+execmethod,
        "\t\tres=$?",
        "\t\tif [ $res -eq 0 ]; then",
        "\t\t\techo success reconnect",
        "\t\t\tbreak",
        "\t\tfi",
        "\tdone",
        "fi",

        # f"ssh -i ~/.ssh/slurm.key shodan@192.168.0.24 'srun ~/cluster/startcontainer.sh'",
        # f"ssh -i ~/.ssh/slurm.key shodan@192.168.0.24 'sbatch --wrap=~/cluster/startcontainer.sh'",
        # "echo starting monitor",
        f"target={job_dir}/done",
        "timeout=0",
        "if [ ! -e $target ]; then",
        "\twhile [ $timeout -ne 30000 ]; do",
        f"\t\ttarget={job_dir}/done",
        "\t\t((timeout=timeout+1))",
        "\t\tsleep 0.1",
        "\t\techo waiting",
        "\t\tif [ -e $target ]; then",
        "\t\t\techo done file found after: $timeout",
        "\t\t\texit 0",
        "\t\tfi",
        "\tdone",
        "fi"

    ]
    return shell + base

def assemble_srun_job_script(params:Dict[str,Any], slurmargs:List[str])->List[str]:
    job_id = params["id"]
    #Could make this an argument aswell with some restrictions
    shell = ["#!/bin/bash"]
    base = [
        "# Start the container",
        f"srun docker run -t -v $(pwd):/meow_base slurm-cluster",
        "retval=$?",
        "if [ $retval -eq 0 ]; then",
        "   echo Job executed succesfully",
        "else",
        "   echo Failed with exitcode: $retval",
        "fi",
        ""
    ]
    return shell + base

def assemble_sbatch_job_script(params:Dict[str,Any], slurmargs:List[str])->List[str]:
    job_id = params["id"]
    #Could make this an argument aswell with some restrictions
    shell = ["#!/bin/bash"]
    base = [
        f"#SBATCH --job-name={job_id}",
        "#SBATCH --output=slurm.out",
        "#SBATCH --error=slurm.err"
        "",
        "# Start the container",
        f"docker run -t -d -v $(pwd):/meow_base slurm-cluster",
        "retval=$?",
        "if [ $retval -eq 0 ]; then",
        "   echo Job executed succesfully",
        "else",
        "   echo Failed with exitcode: $retval",
        "fi",
        "umount -l meow_base",
        ""
    ]
    return shell + slurmargs + base
