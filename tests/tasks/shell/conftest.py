import pytest

from takler.tasks import ShellScriptTask
from takler.core import Flow, Bunch


@pytest.fixture
def shell_task_bunch():
    """
    Flow:

        |- flow1 [unknown]
          |- task1 [unknown]
          |- container1 [unknown]
              |- task2 [unknown]
              |- task3 [unknown]
          |- container2 [unknown]
              |- task4 [unknown]
              |- task5 [unknown]
          |- container3 [unknown]
            |- container4 [unknown]
              |- task6 [unknown]
              |- task7 [unknown]
            |- container5 [unknown]
              |- task8 [unknown]
              |- task9 [unknown]
          |- task10 [unknown]
    """

    class ShellTaskBunch:
        pass

    b = ShellTaskBunch()

    script_home = "/home/johndoe/takler/bunch/flow1"
    takler_home = "/home/johndoe/takler_home/bunch"

    bunch = Bunch()
    bunch.add_parameter("TAKLER_HOME", takler_home)
    b.bunch = bunch

    with Flow("flow1") as flow1:
        b.flow1 = flow1
        bunch.add_flow(flow1)
        with flow1.add_task(
            ShellScriptTask("task1", f"{script_home}/task1.takler")
        ) as task1:
            b.task1 = task1

        with flow1.add_container("container1") as container1:
            b.container1 = container1
            with container1.add_task(
                ShellScriptTask("task2", f"{script_home}/task2.takler")
            ) as task2:
                b.task2 = task2
            with container1.add_task(
                ShellScriptTask("task3", f"{script_home}/task3.takler")
            ) as task3:
                b.task3 = task3

        with flow1.add_container("container2") as container2:
            b.container2 = container2
            with container2.add_task(
                ShellScriptTask("task4", f"{script_home}/task4.takler")
            ) as task4:
                b.task4 = task4
            with container2.add_task(
                ShellScriptTask("task5", f"{script_home}/task5.takler")
            ) as task5:
                b.task5 = task5

        with flow1.add_container("container3") as container3:
            b.container3 = container3
            with container3.add_container("container4") as container4:
                b.container4 = container4
                with container4.add_task(
                    ShellScriptTask("task6", f"{script_home}/container4.takler")
                ) as task6:
                    b.task6 = task6
                with container4.add_task(
                    ShellScriptTask("task7", f"{script_home}/container4.takler")
                ) as task7:
                    b.task7 = task7
            with container3.add_container("container5") as container5:
                b.container5 = container5
                with container5.add_task(
                    ShellScriptTask("task8", f"{script_home}/container5.takler")
                ) as task8:
                    b.task8 = task8
                with container5.add_task(
                    ShellScriptTask("task9", f"{script_home}/container5.takler")
                ) as task9:
                    b.task9 = task9
        with flow1.add_task(
            ShellScriptTask("task10", f"{script_home}/task10.takler")
        ) as task10:
            b.task10 = task10

    return b
