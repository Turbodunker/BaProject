
"""
This file contains the defintion for the MeowRunner, the main construct used 
for actually orchestration MEOW analysis. It is intended as a modular system, 
with monitors, handlers, and conductors being swappable at initialisation.

Author(s): David Marchant
"""
import os
import sys
import threading
import subprocess

from multiprocessing import Pipe
from random import randrange
from typing import Any, Union, Dict, List, Type

from meow_base.core.base_conductor import BaseConductor
from meow_base.core.base_handler import BaseHandler
from meow_base.core.base_monitor import BaseMonitor
from meow_base.core.vars import DEBUG_WARNING, DEBUG_INFO, \
    EVENT_TYPE, VALID_CHANNELS, META_FILE, DEFAULT_JOB_OUTPUT_DIR, \
    DEFAULT_JOB_QUEUE_DIR, EVENT_PATH, JOB_TYPE_BASH, JOB_TYPE_PYTHON, \
    JOB_TYPE_PAPERMILL
from meow_base.functionality.validation import check_type, valid_list, \
    valid_dir_path
from meow_base.functionality.debug import setup_debugging, print_debug
from meow_base.functionality.file_io import make_dir, read_yaml
from meow_base.functionality.process_io import wait


class MeowRunner:
    # A collection of all monitors in the runner
    monitors:List[BaseMonitor]
    # A collection of all handlers in the runner
    handlers:List[BaseHandler]
    # A collection of all conductors in the runner
    conductors:List[BaseConductor]
    # A collection of all channels from each monitor
    from_monitors: List[VALID_CHANNELS]
    # A collection of all channels from each handler
    from_handlers: List[VALID_CHANNELS]
    # Directory where queued jobs are initially written to
    job_queue_dir:str
    # Directory where completed jobs are finally written to
    job_output_dir:str
    def __init__(self, monitors:Union[BaseMonitor,List[BaseMonitor]], 
            handlers:Union[BaseHandler,List[BaseHandler]], 
            conductors:Union[BaseConductor,List[BaseConductor]],
            job_queue_dir:str=DEFAULT_JOB_QUEUE_DIR,
            job_output_dir:str=DEFAULT_JOB_OUTPUT_DIR,
            print:Any=sys.stdout, logging:int=0)->None:
        """MeowRunner constructor. This connects all provided monitors, 
        handlers and conductors according to what events and jobs they produce 
        or consume."""

        self._is_valid_job_queue_dir(job_queue_dir)
        self._is_valid_job_output_dir(job_output_dir)

        self._is_valid_conductors(conductors)
        # If conductors isn't a list, make it one
        if not type(conductors) == list:
            conductors = [conductors]
        for conductor in conductors:
            conductor.job_output_dir = job_output_dir
            conductor.job_queue_dir = job_queue_dir

        self.conductors = conductors

        self._is_valid_handlers(handlers)
        # If handlers isn't a list, make it one
        if not type(handlers) == list:
            handlers = [handlers]
        self.from_handlers = []
        for handler in handlers:            
            # Create a channel from the handler back to this runner
            handler_to_runner_reader, handler_to_runner_writer = Pipe()
            handler.to_runner = handler_to_runner_writer
            handler.job_queue_dir = job_queue_dir
            self.from_handlers.append(handler_to_runner_reader)
        self.handlers = handlers

        self._is_valid_monitors(monitors)
        # If monitors isn't a list, make it one
        if not type(monitors) == list:
            monitors = [monitors]
        self.monitors = monitors
        self.from_monitors = []
        for monitor in self.monitors:
            # Create a channel from the monitor back to this runner
            monitor_to_runner_reader, monitor_to_runner_writer = Pipe()
            monitor.to_runner = monitor_to_runner_writer
            self.from_monitors.append(monitor_to_runner_reader)

        # Create channel to send stop messages to monitor/handler thread
        self._stop_mon_han_pipe = Pipe()
        self._mon_han_worker = None

        # Create channel to send stop messages to handler/conductor thread
        self._stop_han_con_pipe = Pipe()
        self._han_con_worker = None

        # Setup debugging
        self._print_target, self.debug_level = setup_debugging(print, logging)

    def run_monitor_handler_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from monitors. These will be events, which should be matched to an 
        appropriate handler and handled."""
        all_inputs = self.from_monitors + [self._stop_mon_han_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_mon_han_pipe[0] in ready:
                return
            else:
                handled = False
                for from_monitor in self.from_monitors:
                    if from_monitor in ready:
                        # Read event from the monitor channel
                        message = from_monitor.recv()
                        event = message

                        valid_handlers = []
                        for handler in self.handlers:
                            try:
                                valid, _ = handler.valid_handle_criteria(event)
                                if valid:
                                    valid_handlers.append(handler)
                            except Exception as e:
                                print_debug(
                                    self._print_target, 
                                    self.debug_level, 
                                    "Could not determine validity of event "
                                    f"for handler. {e}", 
                                    DEBUG_INFO
                                )

                        # If we've only one handler, use that
                        if len(valid_handlers) == 1:
                            handler = valid_handlers[0]
                            handled = True
                            self.handle_event(handler, event)
                            break
                        # If multiple handlers then randomly pick one
                        elif len(valid_handlers) > 1:
                            handler = valid_handlers[
                                randrange(len(valid_handlers))
                            ]
                            handled = True
                            self.handle_event(handler, event)
                            break

                if not handled:
                    print_debug(
                        self._print_target, 
                        self.debug_level, 
                        "Could not determine handler for event.", 
                        DEBUG_INFO
                    )

    def run_handler_conductor_interaction(self)->None:
        """Function to be run in its own thread, to handle any inbound messages
        from handlers. These will be jobs, which should be matched to an 
        appropriate conductor and executed."""
        all_inputs = self.from_handlers + [self._stop_han_con_pipe[0]]
        while True:
            ready = wait(all_inputs)

            # If we get a message from the stop channel, then finish
            if self._stop_han_con_pipe[0] in ready:
                return
            else:
                executed = False
                for from_handler in self.from_handlers:
                    if from_handler in ready:
                        # Read job directory from the handler channel
                        job_dir = from_handler.recv()
                        try:
                            metafile = os.path.join(job_dir, META_FILE)
                            job = read_yaml(metafile)
                        except Exception as e:
                            print_debug(self._print_target, self.debug_level, 
                                "Could not load necessary job definitions for "
                                f"job at '{job_dir}'. {e}", DEBUG_INFO)
                            continue

                        valid_conductors = []
                        for conductor in self.conductors:
                            try:
                                valid, _ = \
                                    conductor.valid_execute_criteria(job)
                                if valid:
                                    valid_conductors.append(conductor)
                            except Exception as e:
                                print_debug(
                                    self._print_target, 
                                    self.debug_level, 
                                    "Could not determine validity of job "
                                    f"for conductor. {e}", 
                                    DEBUG_INFO
                                )

                        # If we've only one conductor, use that
                        if len(valid_conductors) == 1:
                            conductor = valid_conductors[0]
                            executed = True
                            #Get job_type for translation
                            job_type = job["job_type"]
                            conductor.execute_job(conductor, job_dir)
                            self.translate_job(job_dir, job_type)
                            break
                        # If multiple handlers then randomly pick one
                        elif len(valid_conductors) > 1:
                            conductor = valid_conductors[
                                randrange(len(valid_conductors))
                            ]
                            executed = True
                            self.execute_job(conductor, job_dir)
                            conductor.translate_job(job_dir, job_type)
                            break

                # TODO determine something more useful to do here
                if not executed:
                    print_debug(
                        self._print_target, 
                        self.debug_level, 
                        f"No conductor could be found for job {job_dir}", 
                        DEBUG_INFO
                    )

    def handle_event(self, handler:BaseHandler, event:Dict[str,Any])->None:
        """Function for a given handler to handle a given event, without 
        crashing the runner in the event of a problem."""
        print_debug(self._print_target, self.debug_level, 
            f"Starting handling for {event[EVENT_TYPE]} event: "
            f"'{event[EVENT_PATH]}'", DEBUG_INFO)
        try:
            handler.handle(event)
            print_debug(self._print_target, self.debug_level, 
                f"Completed handling for {event[EVENT_TYPE]} event: "
                f"'{event[EVENT_PATH]}'", DEBUG_INFO)
        except Exception as e:
            print_debug(self._print_target, self.debug_level, 
                f"Something went wrong during handling for {event[EVENT_TYPE]}"
                f" event '{event[EVENT_PATH]}'. {e}", DEBUG_INFO)


    def translate_job(self, conductor:BaseConductor, job_dir:str, job_type:str)->None:
        """Function for a given conductor to translate a given job."""

        job_id = os.path.basename(job_dir)
        if(os.path.exists(f"test_job_output/{job_id}/submit.job")):
            os.remove(f"test_job_output/{job_id}/submit.job")
        #Write the .job file for slurm. Should be scheduled with sbatch in the "home"-directory when mounted.
        with open(f"test_job_output/{job_id}/submit.job", "w+") as fp:
            fp.write("#!/bin/bash\n")
            fp.write("exec $PWD/job.py\n")

            # subprocess.call(["dpipe", "/usr/lib/openssh/sftp-server", "=", "ssh", "mblom@192.168.0.24", "sshfs", ":/home/mblomqvist/Documents/project/meow_base/", "/home/mblom/base", "-o", "slave"])
            # subprocess.run(["/home/mblomqvist/Documents/project/connect.sh"])

    def execute_job(self, conductor:BaseConductor, job_dir:str)->None:
        """Function for a given conductor to execute a given job, without 
        crashing the runner in the event of a problem."""
        job_id = os.path.basename(job_dir)
        print_debug(
            self._print_target, 
            self.debug_level, 
            f"Starting execution for job: '{job_id}'", 
            DEBUG_INFO
        )
        try:
            conductor.execute(job_dir)
            print_debug(
                self._print_target, 
                self.debug_level, 
                f"Completed execution for job: '{job_id}'", 
                DEBUG_INFO
            )
        except Exception as e:
            print_debug(
                self._print_target, 
                self.debug_level, 
                f"Something went wrong in execution of job '{job_id}'. {e}", 
                DEBUG_INFO
            )

    def start(self)->None:
        """Function to start the runner by starting all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""
        # Start all monitors
        for monitor in self.monitors:
            monitor.start()
        startable = []
        # Start all handlers, if they need it
        for handler in self.handlers:
            if hasattr(handler, "start") and handler not in startable:
                startable.append()
        # Start all conductors, if they need it
        for conductor in self.conductors:
            if hasattr(conductor, "start") and conductor not in startable:
                startable.append()
        for starting in startable:
            starting.start()
        
        # If we've not started the monitor/handler interaction thread yet, then
        # do so
        if self._mon_han_worker is None:
            self._mon_han_worker = threading.Thread(
                target=self.run_monitor_handler_interaction,
                args=[])
            self._mon_han_worker.daemon = True
            self._mon_han_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner event handling...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner event handling have " \
                "no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)

        # If we've not started the handler/conductor interaction thread yet, 
        # then do so
        if self._han_con_worker is None:
            self._han_con_worker = threading.Thread(
                target=self.run_handler_conductor_interaction,
                args=[])
            self._han_con_worker.daemon = True
            self._han_con_worker.start()
            print_debug(self._print_target, self.debug_level, 
                "Starting MeowRunner job conducting...", DEBUG_INFO)
        else:
            msg = "Repeated calls to start MeowRunner job conducting have " \
                "no effect."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)

    def stop(self)->None:
        """Function to stop the runner by stopping all of the constituent 
        monitors, handlers and conductors, along with managing interaction 
        threads."""
        # Stop all the monitors
        for monitor in self.monitors:
            monitor.stop()

        stopable = []
        # Stop all handlers, if they need it
        for handler in self.handlers:
            if hasattr(handler, "stop") and handler not in stopable:
                stopable.append()
        # Stop all conductors, if they need it
        for conductor in self.conductors:
            if hasattr(conductor, "stop") and conductor not in stopable:
                stopable.append()
        for stopping in stopable:
            stopping.stop()

        # If we've started the monitor/handler interaction thread, then stop it
        if self._mon_han_worker is None:
            msg = "Cannot stop event handling thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_mon_han_pipe[1].send(1)
            self._mon_han_worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Event handler thread stopped", DEBUG_INFO)

        # If we've started the handler/conductor interaction thread, then stop 
        # it
        if self._han_con_worker is None:
            msg = "Cannot stop job conducting thread that is not started."
            print_debug(self._print_target, self.debug_level, 
                msg, DEBUG_WARNING)
            raise RuntimeWarning(msg)
        else:
            self._stop_han_con_pipe[1].send(1)
            self._han_con_worker.join()
        print_debug(self._print_target, self.debug_level, 
            "Job conductor thread stopped", DEBUG_INFO)

    def get_monitor_by_name(self, queried_name:str)->BaseMonitor:
        """Gets a runner monitor with a name matching the queried name. Note 
        in the case of multiple monitors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.monitors)

    def get_monitor_by_type(self, queried_type:Type)->BaseMonitor:
        """Gets a runner monitor with a type matching the queried type. Note 
        in the case of multiple monitors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.monitors)

    def get_handler_by_name(self, queried_name:str)->BaseHandler:
        """Gets a runner handler with a name matching the queried name. Note 
        in the case of multiple handlers having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.handlers)

    def get_handler_by_type(self, queried_type:Type)->BaseHandler:
        """Gets a runner handler with a type matching the queried type. Note 
        in the case of multiple handlers having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.handlers)

    def get_conductor_by_name(self, queried_name:str)->BaseConductor:
        """Gets a runner conductor with a name matching the queried name. Note 
        in the case of multiple conductors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_name(queried_name, self.conductors)

    def get_conductor_by_type(self, queried_type:Type)->BaseConductor:
        """Gets a runner conductor with a type matching the queried type. Note 
        in the case of multiple conductors having the same name, only the first 
        match is returned."""
        return self._get_entity_by_type(queried_type, self.conductors)

    def _get_entity_by_name(self, queried_name:str, 
            entities:List[Union[BaseMonitor,BaseHandler,BaseConductor]]
            )->Union[BaseMonitor,BaseHandler,BaseConductor]:
        """Base function inherited by more specific name query functions."""
        for entity in entities:
            if entity.name == queried_name:
                return entity
        return None

    def _get_entity_by_type(self, queried_type:Type, 
            entities:List[Union[BaseMonitor,BaseHandler,BaseConductor]]
            )->Union[BaseMonitor,BaseHandler,BaseConductor]:
        """Base function inherited by more specific type query functions."""
        for entity in entities:
            if isinstance(entity, queried_type):
                return entity
        return None

    def _is_valid_monitors(self, 
            monitors:Union[BaseMonitor,List[BaseMonitor]])->None:
        """Validation check for 'monitors' variable from main constructor."""
        check_type(
            monitors, 
            BaseMonitor, 
            alt_types=[List], 
            hint="MeowRunner.monitors"
        )
        if type(monitors) == list:
            valid_list(monitors, BaseMonitor, min_length=1)

    def _is_valid_handlers(self, 
            handlers:Union[BaseHandler,List[BaseHandler]])->None:
        """Validation check for 'handlers' variable from main constructor."""
        check_type(
            handlers, 
            BaseHandler, 
            alt_types=[List], 
            hint="MeowRunner.handlers"
        )
        if type(handlers) == list:
            valid_list(handlers, BaseHandler, min_length=1)

    def _is_valid_conductors(self, 
            conductors:Union[BaseConductor,List[BaseConductor]])->None:
        """Validation check for 'conductors' variable from main constructor."""
        check_type(
            conductors, 
            BaseConductor, 
            alt_types=[List], 
            hint="MeowRunner.conductors"
        )
        if type(conductors) == list:
            valid_list(conductors, BaseConductor, min_length=1)

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
