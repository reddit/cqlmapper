from setuptools import setup, find_packages


setup(
    name="cqlmapper",
    packages=find_packages(),
    install_requires=[
        "cassandra-driver",
    ],
    include_package_data=True,
    tests_require=[
        "mock",
        "nose",
        "coverage",
        "sure",
    ],
)
