import os
import shutil

from setuptools import setup, find_packages

package_name = "vortexpy"
package_version = '3.4.3'

egg_info = "%s.egg-info" % package_name
if os.path.isdir(egg_info):
    shutil.rmtree(egg_info)

requirements = [
    "SQLAlchemy >= 1.0.14",  # Database abstraction layer
    "GeoAlchemy2",  # Geospatial addons to SQLAlchemy
    "txwebsocket>=1.1.0",
    "autobahn[twisted]",
    # txWS requires these, if we try to offline install the packages, txWS setup_requires
    # causes issues
    "vcversioner",
    "six",
    "pytz",
    # RxPY by Microsoft. Used everywhere
    # TODO Upgrade to rx 3.x.x
    "rx < 3.0.0",
    "ddt >=1.4.1",
    # Test requirements
    "psutil==5.9.0",
    "txhttputil>=1.2.0,<1.3.0",
]

setup(
    name="vortexpy",
    packages=find_packages(exclude=["test"]),
    # package_data={'vortex': ['*.xml']},
    version=package_version,
    install_requires=requirements,
    description="Synertys observable, routable, data serialisation and transport code.",
    author="Synerty",
    author_email="contact@synerty.com",
    url="https://github.com/Synerty/vortexpy",
    download_url=(
        "https://github.com/Synerty/%s/tarball/%s"
        % (package_name, package_version)
    ),
    keywords=["vortex", "observable", "http", "compressed", "synerty"],
    classifiers=[
        "Programming Language :: Python :: 3.5",
    ],
)
