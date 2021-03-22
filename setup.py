import os

import setuptools

try:
    with open("README.md", "r") as fh:
        long_description = fh.read()
except FileNotFoundError:
    pass

try:
    with open("requirements-dev.txt", "r") as fh:
        tests_require = [line for line in fh.read().split(os.linesep) if line]
except FileNotFoundError:
    pass

with open("requirements.txt", "r") as fh:
    install_requires = [line for line in fh.read().split(os.linesep) if line]

setuptools.setup(
    name="edgerun-skippy-core",
    version="0.1.0",
    author="Alexander Rashed, Thomas Rausch",
    author_email="alexander.rashed@gmail.com, t.rausch@dsg.tuwien.ac.at",
    description="Skippy scheduler core",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/edgerun/skippy-core",
    download_url="https://github.com/edgerun/skippy-core",
    packages=setuptools.find_packages(),
    setup_requires=['wheel'],
    test_suite="tests",
    tests_require=tests_require,
    install_requires=install_requires,
    pyton_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
    ],
)
