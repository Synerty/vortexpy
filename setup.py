from distutils.core import setup

setup(
    name='vortexpy',
    packages=['vortex', 'vortex.handler'],
    # package_data={'vortex': ['*.xml']},
    version='0.1.0',
    description='Synertys observable, routable, data serialisation and transport code.',
    author='Synerty',
    author_email='contact@synerty.com',
    url='https://github.com/Synerty/vortexpy',
    download_url='https://github.com/Synerty/vortexpy/tarball/0.1.0',
    keywords=['vortex', 'observable', 'http', 'compressed', 'synerty'],
    classifiers=[],
)
