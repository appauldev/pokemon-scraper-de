from setuptools import find_packages, setup

setup(
    name="core",
    packages=find_packages(exclude=["core_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
