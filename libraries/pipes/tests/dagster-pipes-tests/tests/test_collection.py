import os
import shutil
import subprocess


def test_collection(tmpdir_factory):
    current_dir = os.path.dirname(__file__)
    workdir = tmpdir_factory.mktemp("test_collection")

    # copy current_dir/data to workdir

    shutil.copytree(
        os.path.join(current_dir, "data"), os.path.join(workdir), dirs_exist_ok=True
    )

    # rename suite.py to test_suite.py so that pytest can find it

    shutil.move(
        os.path.join(workdir, "suite.py"), os.path.join(workdir, "test_suite.py")
    )

    # simply run pytest --collect-only from the current directory
    # to check that all tests are loaded without errors

    os.chdir(workdir)
    subprocess.run(["pytest"], check=True)
