import pytest

file_test_count = {}

@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    global file_test_count
    file_test_count = {}

@pytest.hookimpl(tryfirst=True)
def pytest_runtestloop(session):
    global file_test_count
    for item in session.items:
        file_path = str(item.fspath)
        if file_path not in file_test_count:
            file_test_count[file_path] = 0
        file_test_count[file_path] += 1

@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(terminalreporter, exitstatus):
    terminalreporter.write_sep("=", "test count summary")
    for file_path, count in file_test_count.items():
        terminalreporter.write_line(f"{file_path}: {count} test(s)")
    terminalreporter.write_line(f"Total number of test files: {len(file_test_count)}")
    terminalreporter.write_line(f"Total number of tests: {sum(file_test_count.values())}")
