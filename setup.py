from setuptools import setup

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(name='pandas_dedupe',
      version='1.5.0',
      description='The Dedupe library made easy with Pandas.',
      url='https://github.com/Lyonk71/pandas-dedupe',
      author='Keith Lyons',
      author_email='lyonk71@gmail.com',
      license='MIT',
      packages=['pandas_dedupe'],
      install_requires=[
          'dedupe>=3.0.0',
          'unidecode',
          'pandas',
      ],
      extras_require={
        "plugins": [
            "dedupe-variable-name>=0.1",
            "dedupe-variable-address>=0.1",
            "dedupe-variable-datetime>=0.1",
            "dedupe-variable-number>=0.1",
        ]
      },
      zip_safe=False,
      
      #Enable pypi description
      long_description=long_description,
      long_description_content_type="text/markdown")
