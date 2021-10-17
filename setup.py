from setuptools import find_packages, setup

setup(
    name="wri-list-tools",
    version="0.1.0",
    description="Python library for working with GFW input and output TSV files",
    author="Eugene Cheipesh",
    author_email="echeipesh@gmail.com",
    url="https://github.com/wri/wri-list-tools",
    license="Apache Software License 2.0",
    packages=find_packages(),
    install_requires=["geopandas>=0.9.0", "pandas>=1.2.4"],
    extras_require={},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)