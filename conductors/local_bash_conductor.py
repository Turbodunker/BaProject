
"""
This file contains definitions for the LocalBashConductor, in order to 
execute Bash jobs on the local resource.

Author(s): David Marchant
"""

import os
from typing import Any, Dict, Tuple, List

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.meow import valid_job
from meow_base.core.vars import DEFAULT_JOB_QUEUE_DIR, \
    DEFAULT_JOB_OUTPUT_DIR, JOB_TYPE, JOB_TYPE_BASH, JOB_TYPE, \
    DEFAULT_JOB_QUEUE_DIR, DEFAULT_JOB_OUTPUT_DIR
from meow_base.functionality.validation import valid_dir_path
from meow_base.functionality.file_io import make_dir, write_file, \
    threadsafe_read_status, threadsafe_update_status, lines_to_string


class LocalBashConductor(BaseConductor):
    def __init__(self, job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR, 
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR, name:str="", 
            pause_time:int=5)->None:
        """LocalBashConductor Constructor. This should be used to execute 
        Bash jobs, and will then pass any internal job runner files to the 
        output directory. Note that if this handler is given to a MeowRunner
        object, the job_queue_dir and job_output_dir will be overwridden."""
        super().__init__(name=name, pause_time=pause_time)
        self._is_valid_job_queue_dir(job_queue_dir)
        self.job_queue_dir = job_queue_dir
        self._is_valid_job_output_dir(job_output_dir)
        self.job_output_dir = job_output_dir

    def valid_execute_criteria(self, job:Dict[str,Any])->Tuple[bool,str]:
        """Function to determine given an job defintion, if this conductor can 
        process it or not. This conductor will accept any Bash job type"""
        try:
            valid_job(job)
            msg = ""
            if job[JOB_TYPE] not in [JOB_TYPE_BASH]:
                msg = f"Job type was not {JOB_TYPE_BASH}."
            if msg:
                return False, msg
            else:
                return True, ""
        except Exception as e:
            return False, str(e)


    def execute(self, job_dir:str)->None:
        """Function to actually execute a Bash job. This will read job 
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
                print(f"PWD: {os.getcwd()}")
                print(f"job_dir: {job_dir}")
                print(os.path.exists(os.path.join(job_dir, get_job_file(JOB_TYPE_BASH))))
                result = subprocess.call(
                    os.path.join(job_dir, get_job_file(JOB_TYPE_BASH)), 
                    cwd="."
                )

                if result == 0:
                    # Update the status file with the finalised status
                    threadsafe_update_status(
                        {
                            JOB_STATUS: STATUS_DONE,
                            JOB_END_TIME: datetime.now()
                        }, 
                        meta_file
                    )

                else:
                    # Update the status file with the error status. Don't 
                    # overwrite any more specific error messages already 
                    # created
                    threadsafe_update_status(
                        {
                            JOB_STATUS: STATUS_FAILED,
                            JOB_END_TIME: datetime.now(),
                            JOB_ERROR: "Job execution returned non-zero."
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

#===========================================================================0


        #Translate to a job-script depending on metafile, parameters and target system
        job_id = os.path.basename(job_dir)
        job_type = ""
        metadata = ""
        print("jobid = ", job_id)
        # Load the YAML file
        # with open(meta_file, "r") as f:
        #      data = yaml.safe_lwith open("example.yaml", "r") as stream:

        # with open(f"test_job_output/{job_id}/job.yml", "r") as f:
        with open(f"test_job_output/{job_id}/job.yml", "r") as f:
            try:
                #TODO:Resolve custom tag issue and use safe_load
                metadata = yaml.load(f, Loader=yaml.Loader)
                job_type = metadata["job_type"]
            except yaml.YAMLError as exc:
                print(exc)

        # job_type = data["job_type"]
        print(job_type)

        if(os.path.exists(f"test_job_output/{job_id}/submit.job")):
            os.remove(f"test_job_output/{job_id}/submit.job")
        #Write the .job file for slurm. Should be scheduled with sbatch in the "home"-directory when mounted.
        slurm_job_script = assemble_slurm_job_script(metadata)
        write_file(lines_to_string(slurm_job_script), f"test_job_output/{job_id}/slurm.job")

        subprocess.call("/home/mblomqvist/Documents/project/connect.sh")

        # with open(f"test_job_output/{job_id}/submit.job", "w+") as fp:
        #     fp.write("#!/bin/bash\n")
        #     fp.write("#SBATCH --job-name=submit_{job_id}.job\n")
        #     fp.write("#SBATCH --output=slurmA.txt\n")
        #     fp.write("OPATH=test_monitor_base/output;\n")
        #     fp.write("cd base\n")
        #     # Call actual job script",
        #     fp.write("")
            #Maybe split this to a seperate function if it grows much more..
            # match job_type:
            #     case "papermill":
            #         fp.write(f"papermill test_job_output/{job_id}/job.ipynb > $OPATH/slurmA.txt\n")
            #     case "python":
            #         fp.write(f"python3 test_job_output/{job_id}/job.py > $OPATH/slurmA.txt\n")
            #     case "bash":
            #         fp.write(f"exec test_job_output/{job_id}/job.sh > $OPATH/slurmA.txt\n")
            #TODO: Make sure the job is finished before unmounting
            # fp.write("umount -l $PWD")


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

def assemble_slurm_job_script(params:Dict[str,Any])->List[str]:
    job_id = params["id"]
    job_type = params["job_type"]
    print(job_id)
    return [
        "#!/bin/bash",
        f"#SBATCH --job-name=submit_{job_id}.job",
        "#SBATCH --output=slurm.out",
        "#SBATCH --error=slurm.err"
        "",
        "# Call actual job script",
        f"./test_job_output/{job_id}/job.sh",
        "retval=$?",
        "if [ $retval -eq 0 ]; then",
        "   echo Job executed succesfully",
        "else",
        "   echo Failed with exitcode: $retval",
        "fi",
        #"umount -l base",
        ""
    ]
