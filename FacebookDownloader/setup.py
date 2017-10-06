from distutils.core import setup

setup(
    # Application name:
    name="fb_stream",

    # Version number (initial):
    version="0.0.1",

    # Application author details:
    author="Alexander Engelhardt \& Tore Christensen",
    author_email="alge@itu.dk \& torc@itu.dk",

    # Packages
    packages=["dk.itu.facebook.stream", 'dk.itu.facebook.bulk'],

    # Include additional files into the package
    include_package_data=True,

    # Details
    #url="http://pypi.python.org/pypi/MyApplication_v010/",

    #
    # license="LICENSE.txt",
    description="Stream posts from facebook to kafka",

    # long_description=open("README.txt").read(),

    # Dependent packages (distributions)
    install_requires=[
        "facebook-sdk",
        "confluent_kafka",
        'python-dateutil'
    ],
)