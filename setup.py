from distutils.core import setup

package_name = "vortexpy"
package_version = '0.1.6'

setup(
    name='vortexpy',
    packages=['vortex', 'vortex.handler'],
    # package_data={'vortex': ['*.xml']},
    version=package_version,
    description='Synertys observable, routable, data serialisation and transport code.',
    author='Synerty',
    author_email='contact@synerty.com',
    url='https://github.com/Synerty/vortexpy',
    download_url=('https://github.com/Synerty/%s/tarball/%s'
                  % (package_name, package_version)),
    keywords=['vortex', 'observable', 'http', 'compressed', 'synerty'],
    classifiers=[
        "Programming Language :: Python :: 3.5",
    ],
)
