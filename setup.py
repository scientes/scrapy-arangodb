from distutils.core import setup

setup(name='scrapyarango',
      version='0.1',
      license='MIT',
      description='Scrapy arangodb extensions',
      author='scientes',
      url='http://github.com/scientes/scrapy-arangodb',
      keywords="scrapy arangodb",
      packages=['scrapyarango'],
      platforms = ['Any'],
      install_requires = ['Scrapy', 'pyarango'],
      classifiers = [ 'Development Status :: 4 - Beta',
                      'License :: OSI Approved :: MIT License',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python']
)
