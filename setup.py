# Modifications Copyright 2016-2017 Reddit, Inc.
#
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import find_packages
from setuptools import setup


setup(
    author="reddit",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
    ],
    description="a fork of cqlengine modified to work in baseplate.py applications",
    include_package_data=True,
    install_requires=["cassandra-driver", "six>=1.6"],
    long_description_content_type="text/x-rst",
    long_description=open("README.rst").read(),
    name="reddit_cqlmapper",
    packages=find_packages(),
    setup_requires=["setuptools_scm"],
    tests_require=["mock", "nose", "coverage"],
    url="https://github.com/reddit/cqlmapper",
    use_scm_version=True,
)
