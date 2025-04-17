from setuptools import setup, find_packages
import os
import re

# Read version without importing the package
with open('papaya/__version__.py', 'r') as f:
    version_match = re.search(r"__version__ = ['\"]([^'\"]*)['\"]", f.read())
    version = version_match.group(1) if version_match else '0.0.0'

# Read README.md for the long description
with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name="papaya",
    version=version,
    description="Automatically create and run a FastMCP server alongside your FastAPI application",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Papaya Contributors",
    author_email="your-email@example.com",  # Replace with your email
    url="https://github.com/yourusername/papaya",  # Replace with your repository URL
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "fastmcp>=0.1.0",  # Adjust version as needed
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={
        "console_scripts": [
            "papaya=papaya.cli:main_cli",
        ],
    },
    python_requires=">=3.7",
)
