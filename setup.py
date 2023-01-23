#!/usr/bin/env python

from distutils.core import setup

setup(
    name="amcat4py",
    version="0.10",
    description="Python client for AmCAT4 API",
    author="Wouter van Atteveldt",
    author_email="wouter@vanatteveldt.com",
    packages=["amcat4py"],
    include_package_data=False,
    zip_safe=False,
    keywords=["API", "text"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Topic :: Text Processing",
    ],
    install_requires=[
        "requests",
        "appdirs",
        "cryptography",
        "requests_oauthlib",
        "tqdm",
    ],
        extras_require={
        'dev': [
            'twine'
        ]

    },
)
