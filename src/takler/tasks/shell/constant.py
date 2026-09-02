# Flow level
TAKLER_INCLUDE = "TAKLER_INCLUDE"

# Task level
TAKLER_SHELL_JOB_CMD = "TAKLER_SHELL_JOB_CMD"
TAKLER_SHELL_KILL_CMD = "TAKLER_SHELL_KILL_CMD"
TAKLER_SCRIPT = "TAKLER_SCRIPT"
TAKLER_JOB = "TAKLER_JOB"
TAKLER_JOBOUT = "TAKLER_JOBOUT"

# Extension
SCRIPT_EXTENSION = "takler"
JOB_SCRIPT_EXTENSION = "job"
JOB_OUTPUT_EXTENSION = "out"
JOB_OUTPUT_ERROR_EXTENSION = "err"

# default command

DEFAULT_TAKLER_SHELL_JOB_CMD = "{{TAKLER_JOB}} 1> {{TAKLER_JOBOUT}} 2>&1"
"""str: default run command for generated job script from ``ShellScriptTask``

Run ``TAKLER_JOB`` script, write stdout and stderr to ``TAKLER_JOBOUT`` file. 
"""

DEFAULT_TAKLER_SHELL_KILL_CMD = "kill -15 {{TAKLER_RID}}"
"""str: default kill command for ``ShellScriptTask``

Kill using process number in ``TAKLER_RID``.
"""
