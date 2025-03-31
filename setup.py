from setuptools import setup, find_packages

setup(
    name="nj-turnpike",
    version="0.1.0",
    description="Custom nodes for the Turnpike framework",
    author="NJ Turnpike Team",
    author_email="example@example.com",
    url="https://github.com/yourusername/nj-turnpike",
    packages=find_packages(),
    install_requires=[
        "turnpike @ git+https://github.com/tnn1t1s/turnpike.git",
        "pyarrow>=7.0.0",
        "ibis-framework",
        "duckdb",
    ],
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)