
import nox

import os


@nox.session
def tests_and_coverage(session):
    """ Install all requirements, run pytest.
    """
    
    session.install("-r", "req.txt")
    session.run("coverage", "run",  "-m",  "--omit=.nox/*", "pytest")
    session.run("coverage", "report", "--fail-under=70", "--show-missing")
    session.run("coverage", "erase")