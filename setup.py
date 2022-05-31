""" @author: Michael Lin """

from setuptools import setup
import io
import os

import utils

here = os.path.abspath(os.path.dirname(__file__))


def read(*filenames, **kwargs):
    encoding = kwargs.get('encoding', 'utf-8')
    sep = kwargs.get('sep', '\n')
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


long_description = read('README.txt', 'CHANGES.txt')


setup(
    name='util',
    version=utils.__version__,
    url='https://github.com/MichaeltheWise/UtilityPackage',
    author='Michael Lin',
    install_requires=['pandas>=1.2.5',
                      'SQLAlchemy>=1.4.34',
                      'psycopg2>=2.9.3',
                      'requests>=2.27.1'],
    author_email='ml145616@gmail.com',
    description='Package building',
    packages=['utils'],
    include_package_data=True,
    platforms='any',
)