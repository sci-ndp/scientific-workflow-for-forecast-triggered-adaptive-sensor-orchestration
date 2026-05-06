from setuptools import setup, find_packages
import os


# Function to read the requirements from requirements.txt
def read_requirements(filename):
    with open(filename, "r") as file:
        lines = file.readlines()
        # Remove comments and empty lines
        requirements = [
            line.strip()
            for line in lines
            if line.strip() and not line.startswith("#")
        ]
    return requirements


# Path to the directory containing setup.py
here = os.path.abspath(os.path.dirname(__file__))


# Read the requirements.txt file
requirements_path = os.path.join(here, "requirements.txt")
install_requires = read_requirements(requirements_path)


# Read the README.md file for the long description
with open(os.path.join(here, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="scidx_streaming",
    version="0.1.7",
    author="Andreu Fornos, Raul Bardaji, Saleem Alharir",
    author_email=(
        "andreu.fornos@utah.edu, rbardaji@gmail.com, saleem.alharir@utah.edu"
    ),
    description=(
        "A Python client library for interacting with the scidx POP and create streams."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sci-ndp/streaming-py",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "scidx_streaming.agentic_mcp": [
            "agent/agent_config.yaml",
            "mcps/catalog/config.yaml",
            "mcps/filtering/config.yaml",
            "mcps/streams/config.yaml",
        ],
    },
    install_requires=install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
