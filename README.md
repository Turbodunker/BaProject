Bachelorsproject in integrating meow_base into slurm. This projects build on top of existing meow_base which is not my work 
The files added are: 
conductors/remote_slurm_conductor.py 
remote/Dockerfile (supposed to be on remote resource) 
remote/docker-entrypoint.sh (supposed to be on remote resource)

The files modified are: 
tests/shared.py look for list COMPLETE_PYTHON_RECIPE_REMOTE also updated paths for TEST_JOB_OUTPUT, TEST_JOB_QUEUE_DIR and TEST_MONITOR_BASE 
tests/test_runner.py look for MeowTests::SweptPythonExecution core/base_handler.py uncommented the hashing-check in def create_job_script_file() and added permissions to job.sh

Configuration files for the remote is found in remote/ and in root for the local

RECENT COMMIT:
Removed the "waiting for done-file" check in connect.sh in the remote_slurm_conductor.py and removed the moving of directories from job-queue to output directory in base_conductor.py
Also changed all instances of "docker" to "podman". This allows for the so called "fire-and-forget-mode" which utilized 2 or more computenodes with sbatch.
