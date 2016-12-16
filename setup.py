from setuptools import setup, find_packages

setup(
    name='ingest',
    description='mongodb mediainfo ingester',
    version='1.0.2',
    packages=find_packages(exclude=['ingest.tests']),
    tests_require=['nose'],
    test_suite='ingest.tests',
    author='Pontus Enmark',
    author_email='pontus@wka.se',
    url='https://github.com/penmark/ingest',
    entry_points={
        'console_scripts': """
            ingest = ingest:from_cmd_line
            ingest_mediainfo = ingest.mediainfo:from_cmd_line
        """
    },
    install_requires="""
        gevent==1.1.2
        lxml==3.4.4
        pillow==3.3.1
        pymongo==3.0.3
        python-magic==0.4.12
        s3_wrapper
        python-dotenv==0.6.1
    """
)
