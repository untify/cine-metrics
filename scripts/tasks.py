import subprocess


def run_command(command, check=True):
    try:
        subprocess.run(command, shell=True, check=check)
    except subprocess.CalledProcessError as e:
        print(f"Command '{command}' failed with exit status {e.returncode}")
        if check:
            raise


def format():
    print("Running Black...")
    run_command("black .")
    print("Running isort...")
    run_command("isort .")


def lint():
    print("Running Ruff...")
    run_command("ruff check .", check=False)


def typecheck():
    print("Running mypy...")
    run_command("mypy .", check=False)


def check():
    format()
    lint()
    typecheck()


if __name__ == "__main__":
    check()
