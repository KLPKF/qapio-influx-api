import sys
from setuptools import setup, find_packages

INSTALL_REQUIRES = [
    'influxdb-client>=1.6.0',
    'pandas>=1.0.3',
]

if sys.platform.startswith('win32'):
    INSTALL_REQUIRES.append("pywin32")

setup(
    name='qapio_influx-api',
    version='0.1.0',
    description='Qapio Influx API',
    long_description=open('README.rst').read(),
    url='https://github.com/tdip/qapio-influx-api',
    author='Turning Data Into Products A.S.',
    author_email='hello@quantifio.no',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    keywords='api qapio',
    packages=find_packages(include=["qapio*"]),
    install_requires=INSTALL_REQUIRES,
    tests_require=['pytest>=2.7.2', 'mock'],
)
