#!/bin/bash
#Mount to the local directory
timeout=0
sshfs mblomqvist@192.168.0.4:/shodan /jail/shodan -o IdentityFile=~/.ssh/remote.key;
res=$?
#Check that the call did not time out
if [ $res -ne 0 ]; then
	while [ $timeout -ne 1000 ]; do
		((timeout=timeout+1))
		echo "sshfs did not complete. retrying: $timeout"
		sleep 0.1
		sshfs mblomqvist@192.168.0.4:/shodan /jail/shodan -o IdentityFile=~/.ssh/remote.key;
		res=$?
		if [ $res -eq 0 ]; then
			echo "succesful mount"
			break
		fi
	done
fi 
cd /jail/shodan
#Run the job script
./meow_base/test_job_queue_dir/$ID/job.sh
echo $?
#Create done file in job directory
touch ./meow_base/test_job_queue_dir/$ID/done
#Unmount 
cd /jail
umount shodan
